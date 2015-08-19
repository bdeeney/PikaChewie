"""
============================================
pikachewie.agent -- PikaChewie agent classes
============================================

"""
import logging
import sys
import time
import traceback
from functools import partial

import pika

from pikachewie import exceptions
from pikachewie.message import Message

log = logging.getLogger(__name__)


class ConsumerAgent(object):
    """
    A RabbitMQ client that passes Messages between a Broker and a Consumer.

    """
    _RECONNECT_DELAY = 5  # seconds

    def __init__(self, consumer, broker, bindings, no_ack=False, config=None):
        self.consumer = consumer
        self.broker = broker
        self.bindings = bindings
        self._ack = not no_ack
        self.config = config or {}
        self.connection = None
        self._reinitialize()

    def _reinitialize(self):
        """Reinitialize the state of this ConsumerAgent."""
        self.channel = None
        self._consumer_tags = {}

    def connect(self):
        """Open a connection to RabbitMQ.

        When the connection is established, the `on_connection_open` method
        will be invoked by pika.

        """
        log.info('Connecting to RabbitMQ via %r', self.broker)
        self.connection = self.broker.connect(
            self.on_connection_open,
            on_failure_callback=self.on_connection_failure
        )

    def on_connection_failure(self, exc):
        """Callback invoked when a RabbitMQ connection cannot be established.

        :param exception exc: the exception raised

        """
        self._record_exception(exc)
        time.sleep(self._RECONNECT_DELAY)
        sys.exit()

    def on_connection_open(self, connection):
        """Callback invoked when a connection to RabbitMQ is established.

        :param connection: the newly opened connection
        :type connection:
            :class:`pika.adapters.tornado_connection.TornadoConnection`

        """
        log.info('Connection opened to %s', self.connection)
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        """Add an on-connection-close callback.

        This method is invoked when the connection is opened.

        """
        log.info('Adding connection close callback')
        self.connection.add_on_close_callback(self.on_connection_close)

    def on_connection_close(self, connection, reply_code, reply_text):
        """
        Callback invoked when the RabbitMQ connection is closed unexpectedly.

        :type connection:
            :class:`pika.adapters.tornado_connection.TornadoConnection`

        """
        log.warning('Server closed connection: (%s) %s', reply_code,
                    reply_text)
        self.reconnect()

    def reconnect(self):
        """Reconnect to RabbitMQ."""
        log.info('Reinitializing...')
        self._reinitialize()
        log.info('Reconnecting in %i seconds', self._RECONNECT_DELAY)
        self.connection.add_timeout(self._RECONNECT_DELAY, self.connect)

    def open_channel(self):
        """Open a new channel on the current connection with RabbitMQ.

        When RabbitMQ responds that the channel is open, the `on_channel_open`
        callback will be invoked.

        """
        log.info('Creating a new channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """Callback invoked when a new channel has been opened.

        :param channel: the newly created channel
        :type channel: :class:`pika.channel.Channel`

        """
        log.info('Channel opened')
        self.channel = channel
        self.add_on_channel_close_callback()
        self.create_bindings()

    def add_on_channel_close_callback(self):
        """Add an on-channel-close callback.

        This method is invoked when the channel is opened.

        """
        log.info('Adding channel close callback')
        self.channel.add_on_close_callback(self.on_channel_close)

    def on_channel_close(self, channel, reply_code, reply_text):
        """Callback invoked when the RabbitMQ channel is unexpectedly closed.

        Channels are usually closed if you attempt to do something that
        violates the protocol, such as redeclare an exchange or queue with
        different paramters. In this case, we'll close the connection
        to shutdown the object.

        :param method_frame: the Channel.Close method frame
        :type method_frame: :class:`pika.frame.Method`

        """
        log.warning('Server closed channel: (%s) %s', reply_code, reply_text)
        self.stop()

    def create_bindings(self):
        """Create a queue binding for each of the Agent's declared bindings."""
        log.info('Creating %d queue binding%s', len(self.bindings),
                 '' if len(self.bindings) == 1 else 's')
        for binding in self.bindings:
            self.create_binding(**binding)

    def create_binding(self, queue, exchange, routing_key):
        """Create a binding using the given queue, exchange, and routing_key.

        This method will declare the exchange and queue.

        """
        log.debug('Creating queue binding: queue=%s, exchange=%s, '
                  'routing_key=%s', queue, exchange, routing_key)
        on_exchange_declare_ok = partial(self.bind, queue, exchange,
                                         routing_key)
        self.declare_exchange(on_exchange_declare_ok, exchange)

    def declare_exchange(self, callback, exchange):
        """Declare the given exchange in RabbitMQ."""
        log.info('Declaring exchange %s', exchange)
        kwargs = self.config.get('exchanges', {}).get(exchange, {})
        self.channel.exchange_declare(callback, exchange, **kwargs)

    def bind(self, queue, exchange, routing_key, method_frame):
        """Declare and bind the given queue to the given exchange.

        The queue is bound to the exchange via the given routing_key.

        """
        on_queue_declare_ok = partial(self.bind_queue, queue, exchange,
                                      routing_key)
        self.declare_queue(on_queue_declare_ok, queue)

    def declare_queue(self, callback, queue):
        """Declare the given queue in RabbitMQ."""
        log.info('Declaring queue %s', queue)
        kwargs = self.config.get('queues', {}).get(queue, {})
        log.debug('Declaring queue %s with %s', queue, kwargs)
        self.channel.queue_declare(callback, queue, **kwargs)

    def bind_queue(self, queue, exchange, routing_key, method_frame):
        """
        Bind the given queue to the given exchange via the given routing_key.

        """
        log.info("Binding queue %s to exchange %s via routing key '%s'",
                 queue, exchange, routing_key)
        on_queue_bind_ok = partial(self.ensure_consuming, queue)
        self.channel.queue_bind(on_queue_bind_ok, queue, exchange, routing_key)

    def ensure_consuming(self, queue, method_frame):
        """Ensure that this agent is consuming from the given `queue`."""
        if not self.is_consuming_from(queue):
            self.start_consuming(queue)

    def is_consuming_from(self, queue):
        """Whether this agent is currently consuming from the given queue.

        :rtype: `bool`

        """
        return queue in self._consumer_tags

    def start_consuming(self, queue):
        """Start consuming messages on the given queue."""
        self.add_on_cancel_callback()
        log.info('Issuing basic_consume on queue %s', queue)
        self._consumer_tags[queue] = self.channel.basic_consume(
            consumer_callback=self.process, queue=queue, no_ack=not self._ack)

    def add_on_cancel_callback(self):
        """Add an on-cancel callback. """
        log.info('Adding consumer cancellation callback')
        self.channel.add_on_cancel_callback(self.on_consumer_cancel)

    def on_consumer_cancel(self, method_frame):
        """Callback invoked when RabbitMQ sends a Basic.Cancel for a consumer.

        :param method_frame: the Basic.Cancel method frame
        :type method_frame: :class:`pika.frame.Method`

        """
        log.info('Consumer was cancelled remotely: %r', method_frame)
        self.reconnect()

    def process(self, channel, method, header, body):
        """Process a message received from RabbitMQ.

        :param channel: the channel the message was received on
        :type channel: :class:`pika.channel.Channel`

        :param method: the method frame
        :type method: :class:`pika.frame.Method`

        :param header: the header frame
        :type header: :class:`pika.frame.Header`

        :param body: the message body
        :type body: str

        """
        message = Message(channel, method, header, body)
        log.debug('Received message #%s', message.delivery_tag)
        log.debug('Message body: %s', message.body)
        if self._process(message):
            if self._ack:
                self.acknowledge(message)

    def _process(self, message):
        """Pass the given message to this agent's consumer for processing."""
        try:
            self.consumer.process(message)

        except exceptions.ConsumerException as exc:
            self._record_exception(exc)
            self.reject(message.delivery_tag)
            return False

        except exceptions.MessageException as exc:
            self._record_exception(exc)
            self.reject(message.delivery_tag, requeue=False)
            return False

        except pika.exceptions.ChannelClosed as exc:
            log.critical('RabbitMQ closed the channel: %r', exc)
            self.reconnect()
            return False

        except pika.exceptions.ConnectionClosed as exc:
            log.critical('RabbitMQ closed the connection: %r', exc)
            self.reconnect()
            return False

        except KeyboardInterrupt:
            self.reject(message.delivery_tag)
            self.stop()
            return False

        return True

    def acknowledge(self, message):
        """Acknowledge delivery of the given message.

        Sends a Basic.Ack RPC method with the message's delivery tag to
        RabbitMQ.

        :param message: the message to acknowledge
        :type message: :class:`pikachewie.message.Message`
        """
        log.debug('Acknowledging message #%s', message.delivery_tag)
        self.channel.basic_ack(message.delivery_tag)

    def run(self):
        """Connect to RabbitMQ and start the connection's IOLoop.

        By starting the IOLoop, this method will block, enabling the
        connection to operate.

        """
        self.connect()
        self.connection.ioloop.start()

    def reject(self, delivery_tag, requeue=True):
        """Reject the message on the broker and log it.

        :param str delivery_tag: delivery tag of the message to reject
        :param bool requeue: whether RabbitMQ should requeue the message

        """
        log.warning('Rejecting message %s %s requeue', delivery_tag,
                    'with' if requeue else 'without')
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ."""
        log.info('Stopping...')
        self.disconnect()
        log.info('Exiting...')

    def disconnect(self):
        """Close the connection to RabbitMQ."""
        if self.connection and self.connection.is_open:
            log.info('Closing connection to %s', self.connection)
            self.connection.close()

    def _record_exception(self, exc):
        """Record an exception

        :param exception exc: the exception to record

        """
        log.warning('Agent handled %s: %s', exc.__class__.__name__, exc)
        if sys.exc_info()[0] is not None:
            formatted_lines = traceback.format_exc()
            for offset, line in enumerate(formatted_lines.splitlines()):
                log.debug('(%s) %i: %s', exc.__class__.__name__, offset,
                          line.strip())
