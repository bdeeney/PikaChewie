from mock import call, MagicMock, NonCallableMagicMock, patch, sentinel
from pika.exceptions import ChannelClosed, ConnectionClosed

from pikachewie.agent import ConsumerAgent
from pikachewie.exceptions import ConsumerException, MessageException
from tests import _BaseTestCase

mod = 'pikachewie.agent'


class DescribeConsumerAgent(_BaseTestCase):

    def execute(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings, sentinel.config)

    def should_have_consumer(self):
        self.assertIs(self.agent.consumer, sentinel.consumer)

    def should_have_broker(self):
        self.assertIs(self.agent.broker, sentinel.broker)

    def should_have_bindings(self):
        self.assertIs(self.agent.bindings, sentinel.bindings)

    def should_have_config(self):
        self.assertIs(self.agent.config, sentinel.config)

    def should_have_default_config(self):
        agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                              sentinel.bindings)
        self.assertEqual(agent.config, {})

    def should_have_connection(self):
        self.assertIsNone(self.agent.connection)

    def should_have_channel(self):
        self.assertIsNone(self.agent.channel)

    def should_have_consumer_tags(self):
        self.assertEqual(self.agent._consumer_tags, {})


class WhenConnectingToBroker(_BaseTestCase):

    def configure(self):
        self.broker = MagicMock()
        self.broker.connect.return_value = sentinel.connection
        self.agent = ConsumerAgent(sentinel.consumer, self.broker,
                                   sentinel.bindings)

    def execute(self):
        self.agent.connect()

    def should_create_broker_connection(self):
        self.broker.connect.assert_called_once_with(
            self.agent.on_connection_open,
            on_failure_callback=self.agent.on_connection_failure
        )

    def should_set_connection(self):
        self.assertIs(self.agent.connection, sentinel.connection)


class DescribeOnConnectionFailure(_BaseTestCase):
    __contexts__ = (
        ('time', patch(mod + '.time')),
        ('sys', patch(mod + '.sys')),
    )

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.add_on_connection_close_callback = MagicMock()
        self.agent.open_channel = MagicMock()

    def execute(self):
        self.agent.on_connection_failure(sentinel.exc)

    def should_sleep(self):
        self.ctx.time.sleep.assert_called_once_with(
            ConsumerAgent._RECONNECT_DELAY
        )

    def should_exit(self):
        self.ctx.sys.exit.assert_called_once_with()


class DescribeOnConnectionOpen(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.add_on_connection_close_callback = MagicMock()
        self.agent.open_channel = MagicMock()

    def execute(self):
        self.agent.on_connection_open(sentinel.connection)

    def should_add_on_connection_close_callback(self):
        self.agent.add_on_connection_close_callback.assert_called_once_with()

    def should_open_channel(self):
        self.agent.open_channel.assert_called_once_with()


class DescribeAddOnConnectionCloseCallback(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.connection = MagicMock()

    def execute(self):
        self.agent.add_on_connection_close_callback()

    def should_add_on_connection_close_callback(self):
        self.agent.connection.add_on_close_callback.assert_called_once_with(
            self.agent.on_connection_close)


class DescribeOnConnectionClose(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.connection = MagicMock()
        self.agent.channel = sentinel.channel
        self.agent._consumer_tags = sentinel._consumer_tags

    def execute(self):
        self.agent.on_connection_close(sentinel.connection,
                                       sentinel.reply_code,
                                       sentinel.reply_text)

    def should_reinitialize_channel(self):
        self.assertIsNone(self.agent.channel)

    def should_reinitialize_consumer_tags(self):
        self.assertEqual(self.agent._consumer_tags, {})

    def should_schedule_reconnection(self):
        self.agent.connection.add_timeout.assert_called_once_with(
            self.agent._RECONNECT_DELAY, self.agent.connect)


class DescribeOpenChannel(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.connection = MagicMock()
        self.agent.on_channel_open = sentinel.on_channel_open

    def execute(self):
        self.agent.open_channel()

    def should_create_channel(self):
        self.agent.connection.channel.assert_called_once_with(
            on_open_callback=sentinel.on_channel_open)


class DescribeOnChannelOpen(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.add_on_channel_close_callback = MagicMock()
        self.agent.create_bindings = MagicMock()

    def execute(self):
        self.agent.on_channel_open(sentinel.channel)

    def should_set_channel(self):
        self.assertIs(self.agent.channel, sentinel.channel)

    def should_add_on_channel_close_callback(self):
        self.agent.add_on_channel_close_callback.assert_called_once_with()

    def should_create_bindings(self):
        self.agent.create_bindings.assert_called_once_with()


class DescribeAddOnChannelCloseCallback(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.add_on_channel_close_callback()

    def should_add_on_connection_close_callback(self):
        self.agent.channel.add_on_close_callback.assert_called_once_with(
            self.agent.on_channel_close)


class DescribeCreateBindings(_BaseTestCase):

    def configure(self):
        self.bindings = (
            {
                'queue': sentinel.queue1,
                'exchange': sentinel.exchange1,
                'routing_key': sentinel.routing_key1,
            },
            {
                'queue': sentinel.queue2,
                'exchange': sentinel.exchange2,
                'routing_key': sentinel.routing_key2,
            },
        )
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   self.bindings)
        self.agent.create_binding = MagicMock()

    def execute(self):
        self.agent.create_bindings()

    def should_create_each_individual_binding(self):
        self.assertEqual(self.agent.create_binding.mock_calls,
                         [call(**binding) for binding in self.bindings])


class DescribeCreateBinding(_BaseTestCase):
    __contexts__ = (
        ('partial', patch(mod + '.partial', return_value=sentinel.callback)),
    )

    def configure(self):
        self.binding = {
            'queue': sentinel.queue,
            'exchange': sentinel.exchange,
            'routing_key': sentinel.routing_key,
        }
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.declare_exchange = MagicMock()

    def execute(self):
        self.agent.create_binding(**self.binding)

    def should_create_on_exchange_declare_ok_callback(self):
        self.ctx.partial.assert_called_once_with(self.agent.bind,
                                                 sentinel.queue,
                                                 sentinel.exchange,
                                                 sentinel.routing_key)

    def should_declare_exchange(self):
        self.agent.declare_exchange.assert_called_once_with(
            sentinel.callback, self.binding['exchange'])


class DescribeDeclareExchange(_BaseTestCase):

    def configure(self):
        self.options = {'exchange_type': 'topic', 'durable': True}
        config = {'exchanges': {sentinel.exchange: self.options}}
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings, config)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.declare_exchange(sentinel.callback, sentinel.exchange)

    def should_declare_exchange(self):
        self.agent.channel.exchange_declare.assert_called_once_with(
            sentinel.callback, sentinel.exchange, **self.options)


class DescribeBind(_BaseTestCase):
    __contexts__ = (
        ('partial', patch(mod + '.partial', return_value=sentinel.callback)),
    )

    def configure(self):
        self.kwargs = {
            'queue': sentinel.queue,
            'exchange': sentinel.exchange,
            'routing_key': sentinel.routing_key,
            'method_frame': sentinel.method_frame
        }
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.declare_queue = MagicMock()

    def execute(self):
        self.agent.bind(**self.kwargs)

    def should_create_on_queue_declare_ok_callback(self):
        self.ctx.partial.assert_called_once_with(self.agent.bind_queue,
                                                 sentinel.queue,
                                                 sentinel.exchange,
                                                 sentinel.routing_key)

    def should_declare_queue(self):
        self.agent.declare_queue.assert_called_once_with(
            sentinel.callback, self.kwargs['queue'])


class DescribeDeclareQueue(_BaseTestCase):

    def configure(self):
        self.options = {'durable': True, 'arguments': {'x-ha-policy': 'all'}}
        config = {'queues': {sentinel.queue: self.options}}
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings, config)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.declare_queue(sentinel.callback, sentinel.queue)

    def should_declare_queue(self):
        self.agent.channel.queue_declare.assert_called_once_with(
            sentinel.callback, sentinel.queue, **self.options)


class DescribeBindQueue(_BaseTestCase):
    __contexts__ = (
        ('partial', patch(mod + '.partial', return_value=sentinel.callback)),
    )

    def configure(self):
        self.kwargs = {
            'queue': sentinel.queue,
            'exchange': sentinel.exchange,
            'routing_key': sentinel.routing_key,
            'method_frame': sentinel.method_frame
        }
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.bind_queue(**self.kwargs)

    def should_create_on_queue_bind_ok_callback(self):
        self.ctx.partial.assert_called_once_with(self.agent.ensure_consuming,
                                                 sentinel.queue)

    def should_bind_queue(self):
        self.agent.channel.queue_bind.assert_called_once_with(
            sentinel.callback, sentinel.queue, sentinel.exchange,
            sentinel.routing_key)


class DescribeIsConsumingFromTrue(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent._consumer_tags = {'my.queue': sentinel.consumer_tag}

    def execute(self):
        self.result = self.agent.is_consuming_from('my.queue')

    def should_return_true(self):
        self.assertTrue(self.result)


class DescribeIsConsumingFromFalse(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent._consumer_tags = {'your.queue': sentinel.consumer_tag}

    def execute(self):
        self.result = self.agent.is_consuming_from('my.queue')

    def should_return_false(self):
        self.assertFalse(self.result)


class DescribeEnsureConsumingNotAlreadyConsuming(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.is_consuming_from = MagicMock(return_value=False)
        self.agent.start_consuming = MagicMock()

    def execute(self):
        self.agent.ensure_consuming(sentinel.queue, sentinel.method_frame)

    def should_check_current_status(self):
        self.agent.is_consuming_from.assert_called_once_with(sentinel.queue)

    def should_start_consuming(self):
        self.agent.start_consuming.assert_called_once_with(sentinel.queue)


class DescribeEnsureConsumingAlreadyConsuming(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.is_consuming_from = MagicMock(return_value=True)
        self.agent.start_consuming = MagicMock()

    def execute(self):
        self.agent.ensure_consuming(sentinel.queue, sentinel.method_frame)

    def should_check_current_status(self):
        self.agent.is_consuming_from.assert_called_once_with(sentinel.queue)

    def should_not_start_consuming(self):
        self.assertFalse(self.agent.start_consuming.called,
                         'ConsumerAgent.start_consuming() was called')


class DescribeStartConsuming(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.add_on_cancel_callback = MagicMock()
        self.agent.channel = MagicMock()
        self.agent.channel.basic_consume.return_value = sentinel.consumer_tag

    def execute(self):
        self.agent.start_consuming(sentinel.queue)

    def should_add_on_cancel_callback(self):
        self.agent.add_on_cancel_callback.assert_called_once_with()

    def should_call_basic_consume(self):
        self.agent.channel.basic_consume.assert_called_once_with(
            consumer_callback=self.agent.process,
            queue=sentinel.queue,
            no_ack=not self.agent._ack)

    def should_set_consumer_tag(self):
        self.assertIs(self.agent._consumer_tags[sentinel.queue],
                      sentinel.consumer_tag)


class DescribeAddOnCancelCallback(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.add_on_cancel_callback()

    def should_add_on_connection_close_callback(self):
        self.agent.channel.add_on_cancel_callback.assert_called_once_with(
            self.agent.on_consumer_cancel)


class DescribeProcess(_BaseTestCase):
    __contexts__ = (
        ('Message', patch(mod + '.Message')),
    )

    def configure(self):
        self.consumer = MagicMock()
        self.ctx.Message.return_value = self.message = NonCallableMagicMock()
        self.agent = ConsumerAgent(self.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.acknowledge = MagicMock()
        self.agent._process = MagicMock()

    def execute(self):
        self.agent.process(sentinel.channel, sentinel.method, sentinel.header,
                           sentinel.body)

    def should_instantiate_message(self):
        self.ctx.Message.assert_called_once_with(
            sentinel.channel, sentinel.method, sentinel.header, sentinel.body)

    def should_call__process(self):
        self.agent._process.assert_called_once_with(self.message)

    def should_acknowledge_message(self):
        self.agent.acknowledge.assert_called_once_with(self.message)


class WhenProcessingMessage(_BaseTestCase):

    def configure(self):
        self.consumer = MagicMock()
        self.message = NonCallableMagicMock()
        self.agent = ConsumerAgent(self.consumer, sentinel.broker,
                                   sentinel.bindings)

    def execute(self):
        self.result = self.agent._process(self.message)

    def should_call_consumer_process(self):
        self.consumer.process.assert_called_once_with(self.message)

    def should_return_true(self):
        self.assertTrue(self.result)


class _BaseProcessingErrorTestCase(_BaseTestCase):

    def configure(self):
        self.consumer = MagicMock()
        self.consumer.process.side_effect = self.exc
        self.message = NonCallableMagicMock()
        self.agent = ConsumerAgent(self.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.reject = MagicMock()
        self.agent.stop = MagicMock()
        self.agent._record_exception = MagicMock()
        self.agent.processing_failure = MagicMock()
        self.agent.reconnect = MagicMock()

    def execute(self):
        self.result = self.agent._process(self.message)

    def should_call_consumer_process(self):
        self.consumer.process.assert_called_once_with(self.message)

    def should_return_false(self):
        self.assertFalse(self.result)


class WhenConsumerExceptionRaised(_BaseProcessingErrorTestCase):
    exc = ConsumerException()

    def should_requeue_message(self):
        self.agent.reject.assert_called_once_with(self.message.delivery_tag)

    def should_record_exception(self):
        self.agent._record_exception.assert_called_once_with(self.exc)


class WhenMessageExceptionRaised(_BaseProcessingErrorTestCase):
    exc = MessageException()

    def should_reject_message(self):
        self.agent.reject.assert_called_once_with(self.message.delivery_tag,
                                                  requeue=False)

    def should_record_exception(self):
        self.agent._record_exception.assert_called_once_with(self.exc)


class WhenChannelClosedRaised(_BaseProcessingErrorTestCase):
    exc = ChannelClosed(sentinel.arg0, sentinel.arg1)

    def should_reconnect(self):
        self.agent.reconnect.assert_called_once_with()


class WhenConnectionClosedRaised(_BaseProcessingErrorTestCase):
    exc = ConnectionClosed(sentinel.arg0, sentinel.arg1)

    def should_reconnect(self):
        self.agent.reconnect.assert_called_once_with()


class WhenKeyboardInterruptRaised(_BaseProcessingErrorTestCase):
    exc = KeyboardInterrupt()

    def should_requeue_message(self):
        self.agent.reject.assert_called_once_with(self.message.delivery_tag)

    def should_stop(self):
        self.agent.stop.assert_called_once_with()


class DescribeAcknowledge(_BaseTestCase):

    def configure(self):
        self.message = MagicMock()
        self.message.delivery_tag = sentinel.delivery_tag
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.channel = MagicMock()

    def execute(self):
        self.agent.acknowledge(self.message)

    def should_send_basic_ack(self):
        self.agent.channel.basic_ack.assert_called_once_with(
            sentinel.delivery_tag)


class DescribeRun(_BaseTestCase):

    def configure(self):
        self.agent = ConsumerAgent(sentinel.consumer, sentinel.broker,
                                   sentinel.bindings)
        self.agent.connect = MagicMock()
        self.agent.connection = MagicMock()

    def execute(self):
        self.agent.run()

    def should_connect(self):
        self.agent.connect.assert_called_once_with()

    def should_start_ioloop(self):
        self.agent.connection.ioloop.start.assert_called_once_with()
