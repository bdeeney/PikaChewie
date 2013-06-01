import bz2
import zlib
from datetime import datetime

import pika
from pika.spec import Basic, BasicProperties
from mock import patch, sentinel
from nose_parameterized import parameterized

from pikachewie.data import Properties
from pikachewie.message import Message
from tests import _BaseTestCase, unittest

mod = 'pikachewie.message'


class DescribeMessage(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
        ('_flush_outbound',
         patch('pika.connection.Connection._flush_outbound')),
    )

    method_delegates = '''
        consumer_tag
        delivery_tag
        exchange
        redelivered
        routing_key
    '''.split()

    properties_delegates = '''
        app_id
        content_encoding
        content_type
        correlation_id
        expiration
        headers
        message_id
        priority
        reply_to
        timestamp
        type
        user_id
    '''.split()

    def configure(self):
        self.consumer_tag = 'R2'
        self.delivery_tag = '1138'
        self.exchange = 'droids'
        self.redelivered = True
        self.routing_key = 'dejarik'

        self.app_id = '3PO'
        self.content_encoding = 'GZIP'
        self.content_type = 'application/JSON'
        self.correlation_id = 'dockingbay94'
        self.expiration = '60000'  # 1 minute TTL
        self.headers = {'tags': ['best practices']}
        self.message_id = '1234'
        self.priority = 9
        self.reply_to = 'reply_to_address'
        self.timestamp = '1234567890'
        self.type = 'message_type_name'
        self.user_id = 'amqp_user_id'

        self.channel = pika.channel.Channel(pika.connection.Connection(),
                                            channel_number=1)
        self.method = Basic.Deliver(**dict([(x, getattr(self, x)) for x in
                                            self.method_delegates]))
        self.body = '{"strategy": "Let the Wookie win."}'
        self.header = BasicProperties(**dict([(x, getattr(self, x)) for x in
                                              self.properties_delegates]))

    def execute(self):
        self.message = Message(self.channel, self.method, self.header,
                               self.body)

    def should_have_channel(self):
        self.assertIs(self.message.channel, self.channel)

    def should_have_method_frame(self):
        self.assertIs(self.message.method, self.method)

    def should_have_properties(self):
        # these multiple assertions are needed because headers are deepcopied
        self.assertIsInstance(self.message.properties, Properties)
        self.assertIs(self.message.properties.app_id, self.app_id)
        self.assertEqual(self.message.properties.headers, self.headers)

    def should_have_body(self):
        self.assertEqual(self.message.body, self.body)

    def should_have_consumer_tag(self):
        self.assertEqual(self.message.consumer_tag, self.consumer_tag)

    def should_have_delivery_tag(self):
        self.assertEqual(self.message.delivery_tag, self.delivery_tag)

    def should_have_exchange(self):
        self.assertEqual(self.message.exchange, self.exchange)

    def should_have_redelivered(self):
        self.assertEqual(self.message.redelivered, self.redelivered)

    def should_have_routing_key(self):
        self.assertEqual(self.message.routing_key, self.routing_key)

    def should_have_app_id(self):
        self.assertEqual(self.message.app_id, self.app_id)

    def should_have_correlation_id(self):
        self.assertEqual(self.message.correlation_id, self.correlation_id)

    def should_have_headers(self):
        self.assertEqual(self.message.headers, self.headers)

    def should_have_id(self):
        self.assertEqual(self.message.id, self.message_id)

    def should_have_priority(self):
        self.assertEqual(self.message.priority, self.priority)

    def should_have_reply_to(self):
        self.assertEqual(self.message.reply_to, self.reply_to)

    def should_have_type(self):
        self.assertEqual(self.message.type, self.type)

    def should_have_user_id(self):
        self.assertEqual(self.message.user_id, self.user_id)

    def should_have_lowercase_content_encoding(self):
        self.assertEqual(self.message.content_encoding, 'gzip')

    def should_have_lowercase_content_type(self):
        self.assertEqual(self.message.content_type, 'application/json')

    def should_have_created_at(self):
        self.assertEqual(self.message.created_at,
                         datetime(2009, 2, 13, 18, 31, 30))

    def should_have_expires_at(self):
        self.assertEqual(self.message.expires_at,
                         datetime(2009, 2, 13, 18, 32, 30))


class WhenMessageCreatedAtUndefined(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
    )

    def configure(self):
        self.header = BasicProperties()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.created_at = self.message.created_at

    def should_return_none(self):
        self.assertIsNone(self.created_at)


class WhenMessageExpiresAtUndefinedDueToCreatedAt(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
    )

    def configure(self):
        self.header = BasicProperties(expiration='60000')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.expires_at = self.message.expires_at

    def should_return_none(self):
        self.assertIsNone(self.expires_at)


class WhenMessageExpiresAtUndefinedDueToExpiration(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
    )

    def configure(self):
        self.header = BasicProperties(timestamp='1234567890')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.expires_at = self.message.expires_at

    def should_return_none(self):
        self.assertIsNone(self.expires_at)


class WhenMessageDoesNotExpire(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
    )

    def configure(self):
        self.header = BasicProperties()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_not_be_expired(self):
        self.assertIs(self.is_expired, False)


class WhenMessageHasNotExpired(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
        ('datetime', patch(mod + '.datetime', wraps=datetime)),
    )

    def configure(self):
        self.ctx.datetime.now.return_value = datetime(2009, 2, 13, 18, 32, 29)
        self.header = BasicProperties(timestamp='1234567890',
                                      expiration='60000')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_not_be_expired(self):
        self.assertIs(self.is_expired, False)


class WhenMessageHasExpired(_BaseTestCase):
    __contexts__ = (
        ('_adapter_connect',
         patch('pika.connection.Connection._adapter_connect')),
        ('datetime', patch(mod + '.datetime', wraps=datetime)),
    )

    def configure(self):
        self.ctx.datetime.now.return_value = datetime(2009, 2, 13, 18, 32, 30)
        self.header = BasicProperties(timestamp='1234567890',
                                      expiration='60000')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_not_be_expired(self):
        self.assertIs(self.is_expired, True)


@patch('pika.connection.Connection._adapter_connect')
class WhenGettingPayload(unittest.TestCase):
    body = '{"strategy": "Let the Wookie win."}'

    @parameterized.expand((
        ('plain text', {}, body, body),
        ('bzip2', {'content_encoding': 'bzip2'}, bz2.compress(body), body),
        ('gzip', {'content_encoding': 'gzip'}, zlib.compress(body), body),
        ('json', {'content_type': 'application/json'}, body,
         {'strategy': 'Let the Wookie win.'}),
    ))
    def should_return_expected_payload(self, name, headers, body, expected):
        self.header = BasicProperties(**headers)
        message = Message(sentinel.channel, sentinel.method, self.header, body)
        self.assertEqual(message.payload, expected)
