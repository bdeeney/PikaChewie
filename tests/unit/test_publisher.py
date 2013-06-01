from mock import call, MagicMock, patch, PropertyMock, sentinel
from pika.exceptions import ConnectionClosed, ChannelClosed

from pikachewie.publisher import BlockingJSONPublisher, BlockingPublisher
from tests import _BaseTestCase

mod = 'pikachewie.publisher'


class DescribeBlockingPublisher(_BaseTestCase):

    def configure(self):
        self.broker = MagicMock()
        self.broker.connect.return_value = connection = MagicMock()
        connection.channel.return_value = self.channel = MagicMock()

    def execute(self):
        self.publisher = BlockingPublisher(self.broker)

    def should_have_broker(self):
        self.assertIs(self.publisher.broker, self.broker)

    def should_have_channel(self):
        self.assertIs(self.publisher.channel, self.channel)


class WhenPublishingMessage(_BaseTestCase):

    def configure(self):
        self.publisher = BlockingPublisher(sentinel.broker)
        self.publisher._channel = self.channel = MagicMock(is_open=True)
        self.publisher._build_basic_properties = \
            MagicMock(return_value=sentinel.basic_properties)

    def execute(self):
        self.publisher.publish(sentinel.exchange, sentinel.routing_key,
                               sentinel.body, sentinel.properties)

    def should_build_basic_properties(self):
        self.publisher._build_basic_properties.assert_called_once_with(
            sentinel.properties)

    def should_call_basic_publish(self):
        self.channel.basic_publish.assert_called_once_with(
            exchange=sentinel.exchange, routing_key=sentinel.routing_key,
            body=sentinel.body, properties=sentinel.basic_properties)


class _BasePublishOnClosedResource(_BaseTestCase):

    def configure(self):
        self.publisher = BlockingPublisher(MagicMock())
        BlockingPublisher.channel = self.channel = PropertyMock(is_open=True)
        self.channel().basic_publish = \
            MagicMock(side_effect=[self.exception_cls(), None])
        self.publisher._build_basic_properties = \
            MagicMock(return_value=sentinel.basic_properties)

    def execute(self):
        self.publisher.publish(sentinel.exchange, sentinel.routing_key,
                               sentinel.body, sentinel.properties)

    def should_build_basic_properties(self):
        self.publisher._build_basic_properties.assert_called_once_with(
            sentinel.properties)

    def should_call_basic_publish_twice(self):
        basic_publish_call = call(
            exchange=sentinel.exchange,
            routing_key=sentinel.routing_key,
            body=sentinel.body,
            properties=sentinel.basic_properties)
        self.assertEqual(self.channel().basic_publish.mock_calls,
                         [basic_publish_call] * 2)


class WhenPublishingMessageOnClosedChannel(_BasePublishOnClosedResource):
    exception_cls = ChannelClosed


class WhenPublishingMessageOnClosedConnection(_BasePublishOnClosedResource):
    exception_cls = ConnectionClosed


class DescribeJSONBlockingPublisher(_BaseTestCase):

    def should_extend_BlockingPublisher(self):
        isinstance(BlockingJSONPublisher(sentinel.broker), BlockingPublisher)


class WhenPublishingJSONMessage(_BaseTestCase):
    __contexts__ = (
        ('super', patch(mod + '.super', create=True)),
    )

    def configure(self):
        self.ctx.super.return_value = self.superclass = MagicMock()
        self.publisher = BlockingJSONPublisher(sentinel.broker)
        self.publisher._serialize = MagicMock(return_value=sentinel.body)
        self.publisher._channel = MagicMock(is_open=True)
        self.properties = MagicMock(content_type=None)

    def execute(self):
        self.publisher.publish(sentinel.exchange, sentinel.routing_key,
                               sentinel.payload, self.properties)

    def should_serialize_payload(self):
        self.publisher._serialize.assert_called_once_with(sentinel.payload)

    def should_call_super(self):
        self.ctx.super.assert_called_once_with(BlockingJSONPublisher,
                                               self.publisher)

    def should_call_publish_on_superclass(self):
        self.superclass.publish.assert_called_once_with(sentinel.exchange,
                                                        sentinel.routing_key,
                                                        sentinel.body,
                                                        self.properties)

    def should_set_content_type(self):
        self.assertEqual(self.properties.content_type, 'application/json')
