"""
======================================
pikachewie.broker -- A RabbitMQ broker
======================================

"""
import logging
import random
import time

from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.adapters.tornado_connection import TornadoConnection
from pika.exceptions import AMQPConnectionError

from pikachewie.utils import Missing

__all__ = ['Broker']

log = logging.getLogger(__name__)


class BrokerConnectionError(AMQPConnectionError):
    """Raised when a new connection cannot be opened to a ``Broker``."""
    pass


class Broker(object):
    """A RabbitMQ broker.

    `nodes` is a two-level :class:`dict` containing the names and network
    locations of the clustered nodes that comprise the RabbitMQ broker,
    e.g.::

        {
            'bunny1': {'host': 'localhost'},
            'bunny2': {'host': 'rabbit.example.com', 'port': 5678}
        }

    Valid netloc parameters are `host` and `port`.  If not specified,
    `nodes` defaults to ``{'default': {}}``, which represents a broker
    consisting of one node named "default" located at ``localhost:5672``.

    `connect_options` is a :class:`dict` of keywords arguments to be passed to
    :class:`pika.connection.ConnectionParameters`.  If not specified, the
    :mod:`pika` default connection parameters will be used.  For the list of
    supported keyword arguments and their default values, see
    :class:`pika.connection.ConnectionParameters`.

    :param nodes: two-level dict of nodenames and node netloc parameters
    :type nodes: :class:`dict`
    :param connect_options: dict of
        :class:`pika.connection.ConnectionParameters` keyword arguments
    :type connect_options: :class:`dict`

    """

    DEFAULT_NODES = {'default': {}}
    DEFAULT_CONNECT_OPTIONS = {}
    _attempts = None
    _max_attempts = None
    _abort_on_error = False

    def __init__(self, nodes=None, connect_options=None):
        if nodes is None:
            nodes = self.DEFAULT_NODES
        if connect_options is None:
            connect_options = self.DEFAULT_CONNECT_OPTIONS

        self._nodes = list(nodes.items())
        self._connect_options = connect_options.copy()
        if isinstance(self._connect_options.get('credentials'), dict):
            self._connect_options['credentials'] = PlainCredentials(
                **self._connect_options['credentials'])

    def connect(self, on_open_callback=None, stop_ioloop_on_close=False,
                connection_attempts=Missing, on_failure_callback=None,
                cycle_delay=None, blocking=False):
        """
        Return a new connection to this broker.

        This method connects to each node of the broker in turn, until either a
        connection is successfully established, or the number of total
        `connection_attempts` is reached.  The order in which the nodes are
        tried is random, and potentially different between different calls to
        :meth:`connect`.

        If `connection_attempts` is greater than the number of broker nodes,
        this method cycles through the list of nodes repeatedly as needed,
        pausing for `cycle_delay` seconds between each pass through the node
        list.

        If `connection_attempts` is not specified, it defaults to the number of
        broker nodes, so that each node is tried at most once.  If
        `connection_attempts` is explicitly set to `None`, there is no limit on
        the number of connection attempts, and :meth:`connect` blocks until
        a connection is successfully made.

        When `connection_attempts` is reached, the resultant behavior depends
        on whether an `on_failure_callback` has been specified.  If it is
        defined, the `on_failure_callback` is called with a
        :class:`BrokerConnectionError` as its sole argument, and
        :meth:`connect` returns `None`.  Otherwise, :meth:`connect` raises
        the :class:`BrokerConnectionError`.

        :param on_open_callback: invoked once the AMQP connection is opened.
            Note: this parameter is ignored if `blocking` is True.
        :type on_open_callback: callable

        :param stop_ioloop_on_close: whether to stop the IOLoop when the
            connection is closed (default: ``True``).  Note: this parameter is
            ignored if `blocking` is True.
        :type stop_ioloop_on_close: :class:`bool`

        :param connection_attempts: maximum numbers of total connection
            attempts to make before failing
        :type connection_attempts: :class:`int` or :class:`NoneType`

        :param on_failure_callback: invoked if the connection attempt fails
        :type on_failure_callback: callable

        :param cycle_delay: number of seconds to sleep between each cycle
            through the nodes list
        :type cycle_delay: :class:`float` or :class:`NoneType`

        :param blocking: if True, return a :class:`pika.BlockingConnection`
            (default: ``False``)
        :type blocking: :class:`bool`

        :returns: new connection to this broker (or ``None``)
        :rtype: :class:`pika.adapters.tornado_connection.TornadoConnection` or
            :class:`pika.BlockingConnection` or :class:`NoneType`
        :raises: :class:`BrokerConnectionError`

        """
        self._initialize_connection_attempt(connection_attempts)

        while True:
            for nodename, node in self._nodes:
                self._increment_connection_attempts()
                parameters = self._get_connection_parameters(node)
                log.info('Connecting to %r via node "%s"', self, nodename)
                log.debug('Opening connection with %r', parameters)

                try:
                    if blocking:
                        return BlockingConnection(parameters)
                    else:
                        return TornadoConnection(
                            parameters,
                            on_open_callback=on_open_callback,
                            stop_ioloop_on_close=stop_ioloop_on_close,
                        )
                except AMQPConnectionError as exc:
                    log.warn('Cannot connect to node %s: %s: %s', nodename,
                             type(exc).__name__, exc)
                    if self._abort_on_error:
                        self._handle_connect_failure(on_failure_callback)
                        return

            if cycle_delay:
                log.info('Sleeping for %s second%s...', cycle_delay,
                         '' if str(cycle_delay) == '1' else 's')
                time.sleep(cycle_delay)

    def _initialize_connection_attempt(self, max_attempts):
        """Prepare this broker for a connection attempt."""
        random.shuffle(self._nodes)
        if max_attempts is Missing:
            max_attempts = len(self._nodes)
        self._max_attempts = max_attempts
        self._attempts = 0
        self._abort_on_error = False

    def _increment_connection_attempts(self):
        """Increment the number of connection attempts.

        Adds 1 to the count of connection attempts made during the current call
        to :meth:`connect`.

        """
        self._attempts += 1
        if self._max_attempts and self._attempts >= self._max_attempts:
            self._abort_on_error = True

    def _get_connection_parameters(self, node_parameters):
        """Return parameters for connecting to a RabbitMQ node."""
        params = self._connect_options.copy()
        params.update(node_parameters)
        return ConnectionParameters(**params)

    def _handle_connect_failure(self, on_failure_callback):
        """Handle the failure of a call to :meth:`connect`.

        Creates a ``BrokerConnectionError`` exception object.  If an
        `on_failure_callback` is specified, this method invokes the callback
         with the exception as its sole argument.  Otherwise, this method
         raises the exception.


        """
        log.warn('Failed to connect to %s in %d attempt%s', self,
                 self._attempts, '' if str(self._attempts) == '1' else 's')
        exc = BrokerConnectionError(self._attempts)
        if on_failure_callback:
            log.info('Calling on_failure callback function %r',
                     on_failure_callback)
            on_failure_callback(exc)
        else:
            raise exc
