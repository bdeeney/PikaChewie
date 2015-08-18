from mock import MagicMock, NonCallableMagicMock, PropertyMock, patch, sentinel
from pika.adapters.tornado_connection import TornadoConnection
from pika.exceptions import AMQPConnectionError

from pikachewie.broker import Broker
from pikachewie.utils import cached_property
from tests import _BaseTestCase, unittest

mod = 'pikachewie.broker'
sut = mod + '.Broker'


class DescribeBroker(unittest.TestCase):

    def should_have_default_nodes(self):
        self.assertEqual(Broker.DEFAULT_NODES, {'default': {}})

    def should_have_default_connection_options(self):
        self.assertEqual(Broker.DEFAULT_CONNECT_OPTIONS, {})


class _BrokerInitTestCase(_BaseTestCase):
    __contexts__ = (
        ('DEFAULT_NODES', patch(sut + '.DEFAULT_NODES')),
        ('DEFAULT_CONNECT_OPTIONS', patch(sut + '.DEFAULT_CONNECT_OPTIONS')),
    )

    def configure(self):
        self.node_tuples = {'default': {}}.items()
        self.nodes.items.return_value = self.node_tuples
        self.connect_options_copy = MagicMock(spec=dict)
        self.connect_options.copy.return_value = self.connect_options_copy

    def should_get_list_of_nodes(self):
        self.nodes.items.assert_called_once_with()

    def should_set_nodes(self):
        self.assertEqual(self.broker._nodes, list(self.node_tuples))

    def should_copy_connect_options(self):
        self.connect_options.copy.assert_called_once_with()

    def should_set_connect_options(self):
        self.assertEqual(self.broker._connect_options,
                         self.connect_options_copy)


class WhenCreatingBrokerWithoutArguments(_BrokerInitTestCase):

    @property
    def nodes(self):
        return self.ctx.DEFAULT_NODES

    @property
    def connect_options(self):
        return self.ctx.DEFAULT_CONNECT_OPTIONS

    def execute(self):
        self.broker = Broker()


class WhenCreatingBrokerWithNodes(_BrokerInitTestCase):

    @cached_property
    def nodes(self):
        return MagicMock()

    @property
    def connect_options(self):
        return self.ctx.DEFAULT_CONNECT_OPTIONS

    def execute(self):
        self.broker = Broker(self.nodes)


class WhenCreatingBrokerWithConnectOptions(_BrokerInitTestCase):

    @property
    def nodes(self):
        return self.ctx.DEFAULT_NODES

    @cached_property
    def connect_options(self):
        return MagicMock()

    def execute(self):
        self.broker = Broker(None, self.connect_options)


class WhenCreatingBrokerWithCredentialsDict(_BaseTestCase):
    __contexts__ = (
        ('DEFAULT_NODES', patch(sut + '.DEFAULT_NODES')),
        ('DEFAULT_CONNECT_OPTIONS', patch(sut + '.DEFAULT_CONNECT_OPTIONS')),
        ('PlainCredentials', patch(mod + '.PlainCredentials',
                                   return_value=sentinel.plain_credentials)),
    )
    credentials = {
        'username': sentinel.username,
        'password': sentinel.password,
    }

    @property
    def nodes(self):
        return self.ctx.DEFAULT_NODES

    @cached_property
    def connect_options(self):
        return NonCallableMagicMock()

    def configure(self):
        self.node_tuples = {'default': {}}.items()
        self.nodes.items.return_value = self.node_tuples
        self.connect_options.copy.return_value = {
            'credentials': self.credentials,
        }

    def execute(self):
        self.broker = Broker(None, self.connect_options)

    def should_create_plain_credentials(self):
        self.ctx.PlainCredentials.assert_called_once_with(**self.credentials)

    def should_store_plain_credentials_in_connect_options(self):
        self.assertEqual(self.broker._connect_options['credentials'],
                         sentinel.plain_credentials)


class WhenGettingAsynchronousConnection(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    return_value=sentinel.connection)),
        ('_get_connection_parameters',
         patch(sut + '._get_connection_parameters',
               return_value=sentinel.parameters)),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )
    nodes = {'localhost': {'host': '127.0.0.1'}}

    def configure(self):
        self.broker = Broker(self.nodes)

    def execute(self):
        self.connection = self.broker.connect(
            connection_attempts=sentinel.connection_attempts)

    def should_initialize_connection_attempt(self):
        self.ctx._initialize_connection_attempt.assert_called_once_with(
            sentinel.connection_attempts)

    def should_increment_connection_attempts(self):
        self.ctx._increment_connection_attempts.assert_called_once_with()

    def should_get_connection_parameters(self):
        self.ctx._get_connection_parameters.assert_called_once_with(
            self.nodes['localhost'])

    def should_create_connection(self):
        self.ctx.TornadoConnection.assert_called_once_with(
            sentinel.parameters,
            on_open_callback=None,
            stop_ioloop_on_close=False)

    def should_return_connection(self):
        self.assertIs(self.connection, sentinel.connection)


class WhenGettingSynchronousConnection(_BaseTestCase):
    __contexts__ = (
        ('BlockingConnection', patch(mod + '.BlockingConnection',
                                     return_value=sentinel.connection)),
        ('_get_connection_parameters',
         patch(sut + '._get_connection_parameters',
               return_value=sentinel.parameters)),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )
    nodes = {'localhost': {'host': '127.0.0.1'}}

    def configure(self):
        self.broker = Broker(self.nodes)

    def execute(self):
        self.connection = self.broker.connect(
            connection_attempts=sentinel.connection_attempts, blocking=True)

    def should_initialize_connection_attempt(self):
        self.ctx._initialize_connection_attempt.assert_called_once_with(
            sentinel.connection_attempts)

    def should_increment_connection_attempts(self):
        self.ctx._increment_connection_attempts.assert_called_once_with()

    def should_get_connection_parameters(self):
        self.ctx._get_connection_parameters.assert_called_once_with(
            self.nodes['localhost'])

    def should_create_connection(self):
        self.ctx.BlockingConnection.assert_called_once_with(
            sentinel.parameters)

    def should_return_connection(self):
        self.assertIs(self.connection, sentinel.connection)


class WhenGettingConnectionParameters(_BaseTestCase):
    __contexts__ = (
        ('ConnectionParameters', patch(mod + '.ConnectionParameters',
                                       return_value=sentinel.parameters)),
    )

    def configure(self):
        self.broker = Broker()
        self.options = MagicMock()
        self.broker._connect_options = NonCallableMagicMock()
        self.broker._connect_options.copy.return_value = self.options

    def execute(self):
        self.parameters = self.broker._get_connection_parameters(
            sentinel.node_parameters)

    def should_create_connection_parameters(self):
        self.ctx.ConnectionParameters.assert_called_once_with(**self.options)

    def should_return_connection_parameters(self):
        self.assertIs(self.parameters, sentinel.parameters)

    def should_copy_connect_options(self):
        self.broker._connect_options.copy.assert_called_once_with()

    def should_merge_node_parameters(self):
        self.options.update.assert_called_once_with(sentinel.node_parameters)


class WhenConnectingWithOnOpenCallback(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    return_value=sentinel.connection)),
        ('_get_connection_parameters',
         patch(sut + '._get_connection_parameters',
               return_value=sentinel.parameters)),
    )

    def configure(self):
        self.ctx.TornadoConnection.return_value = sentinel.connection
        self.broker = Broker()
        self.on_open_callback = MagicMock()

    def execute(self):
        self.connection = self.broker.connect(self.on_open_callback)

    def should_create_connection_with_callback(self):
        self.ctx.TornadoConnection.assert_called_once_with(
            sentinel.parameters,
            on_open_callback=self.on_open_callback,
            stop_ioloop_on_close=False)


class WhenEncounteringConnectionError(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    side_effect=[AMQPConnectionError(1),
                                                 TornadoConnection])),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )
    nodes = {'rabbit1': {}, 'rabbit2': {}}

    def configure(self):
        self.broker = Broker(self.nodes)
        self.broker._nodes = PropertyMock()
        self.broker._nodes.__iter__ = MagicMock(
            return_value=iter(self.nodes.items()))

    def execute(self):
        self.connection = self.broker.connect()

    def should_iterate_over_nodes(self):
        self.broker._nodes.__iter__.assert_called_once_with()


class WhenExhaustingNodeList(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    side_effect=[AMQPConnectionError(1),
                                                 AMQPConnectionError(1),
                                                 TornadoConnection])),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )
    nodes = {'rabbit1': {}, 'rabbit2': {}}

    def configure(self):
        self.broker = Broker(self.nodes)
        self.broker._nodes = PropertyMock()
        get_node_iterator = lambda: iter(self.nodes.items())
        self.broker._nodes.__iter__ = MagicMock(side_effect=get_node_iterator)

    def execute(self):
        self.connection = self.broker.connect()

    def should_recycle_nodes(self):
        self.assertEqual(len(self.broker._nodes.__iter__.mock_calls), 2)


class WhenRecyclingNodesWithDelay(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    side_effect=[AMQPConnectionError(1),
                                                 AMQPConnectionError(1),
                                                 TornadoConnection])),
        ('sleep', patch(mod + '.time.sleep')),
    )

    def configure(self):
        self.ctx.TornadoConnection._adapter_connect = MagicMock()
        self.broker = Broker({'rabbit1': {}, 'rabbit2': {}})

    def execute(self):
        self.connection = self.broker.connect(connection_attempts=5,
                                              cycle_delay=sentinel.delay)

    def should_sleep(self):
        self.ctx.sleep.assert_called_once_with(sentinel.delay)


class WhenInitializingConnectionAttempt(_BaseTestCase):
    __contexts__ = (
        ('shuffle', patch(mod + '.random.shuffle')),
    )

    def configure(self):
        self.broker = Broker()
        self.broker._nodes = sentinel.nodes
        self.broker._abort_on_error = True

    def execute(self):
        self.broker._initialize_connection_attempt(
            sentinel.connection_attempts)

    def should_shuffle_nodes(self):
        self.ctx.shuffle.assert_called_once_with(sentinel.nodes)

    def should_set_attempts(self):
        self.assertEqual(self.broker._attempts, 0)

    def should_set_max_attempts(self):
        self.assertEqual(self.broker._max_attempts,
                         sentinel.connection_attempts)

    def should_initialize_abort_on_error_flag(self):
        self.assertFalse(self.broker._abort_on_error)


class WhenIncrementingConnectionAttempts(_BaseTestCase):

    def configure(self):
        self.broker = Broker()
        self.broker._initialize_connection_attempt(max_attempts=2)

    def execute(self):
        self.broker._increment_connection_attempts()

    def should_increment_attempts(self):
        self.assertEqual(self.broker._attempts, 1)

    def should_preserve_abort_on_error_flag(self):
        self.assertFalse(self.broker._abort_on_error)


class WhenReachingMaximumConnectionAttempts(_BaseTestCase):

    def configure(self):
        self.broker = Broker()
        self.broker._initialize_connection_attempt(max_attempts=1)

    def execute(self):
        self.broker._increment_connection_attempts()

    def should_activate_abort_on_error_flag(self):
        self.assertTrue(self.broker._abort_on_error)


class WhenUsingUnlimitedConnectionAttempts(_BaseTestCase):

    def configure(self):
        self.broker = Broker()
        self.broker._initialize_connection_attempt(max_attempts=None)

    def execute(self):
        self.broker._increment_connection_attempts()

    def should_preserve_abort_on_error_flag(self):
        self.assertFalse(self.broker._abort_on_error)


class WhenAbortingConnectionAttempt(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    side_effect=AMQPConnectionError(1))),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )

    def setUp(self):
        pass

    def configure(self):
        self.broker = Broker()
        self.broker._abort_on_error = True
        self.broker._attempts = 1

    def should_raise_exception(self):
        with self.context() as self.ctx:
            self.configure()
            with self.assertRaisesRegexp(AMQPConnectionError, '^1$'):
                self.broker.connect()


class WhenAbortingConnectionAttemptWithCallback(_BaseTestCase):
    __contexts__ = (
        ('TornadoConnection', patch(mod + '.TornadoConnection',
                                    side_effect=AMQPConnectionError(1))),
        ('BrokerConnectionError', patch(mod + '.BrokerConnectionError')),
        ('_initialize_connection_attempt',
         patch(sut + '._initialize_connection_attempt')),
        ('_increment_connection_attempts',
         patch(sut + '._increment_connection_attempts')),
    )

    def configure(self):
        self.broker = Broker()
        self.broker._abort_on_error = True
        self.broker._attempts = 1
        self.on_failure_callback = MagicMock(autospec=True)
        self.ctx.BrokerConnectionError.return_value = sentinel.exception

    def execute(self):
        self.broker.connect(on_failure_callback=self.on_failure_callback)

    def should_invoke_on_failure_callback(self):
        self.on_failure_callback.assert_called_once_with(sentinel.exception)
