from mock import patch, sentinel
from pika.spec import BasicProperties

from pikachewie.data import Properties
from tests import _BaseTestCase, unittest

mod = 'pikachewie.data'


class DescribeProperties(_BaseTestCase):
    __contexts__ = (
        ('uuid4', patch(mod + '.uuid.uuid4', return_value=sentinel.uuid)),
        ('str', patch(mod + '.str', create=True,
                      return_value=sentinel.message_id)),
        ('time', patch(mod + '.time.time', return_value=sentinel.epoch)),
        ('int', patch(mod + '.int', create=True,
                      return_value=sentinel.timestamp)),
    )

    def execute(self):
        self.properties = Properties()

    def should_have_default_app_id(self):
        self.assertIsNone(self.properties.app_id)

    def should_have_default_cluster_id(self):
        self.assertIsNone(self.properties.cluster_id)

    def should_have_default_content_encoding(self):
        self.assertIsNone(self.properties.content_encoding)

    def should_have_default_content_type(self):
        self.assertEqual(self.properties.content_type, 'text/text')

    def should_have_default_correlation_id(self):
        self.assertIsNone(self.properties.correlation_id)

    def should_have_default_delivery_mode(self):
        self.assertEqual(self.properties.delivery_mode, 1)

    def should_have_default_expiration(self):
        self.assertIsNone(self.properties.expiration)

    def should_have_default_headers(self):
        self.assertEqual(self.properties.headers, {})

    def should_create_uuid(self):
        self.ctx.uuid4.assert_called_once_with()

    def should_stringify_uuid(self):
        self.ctx.str.assert_called_once_with(sentinel.uuid)

    def should_set_message_id(self):
        self.assertIs(self.properties.message_id, sentinel.message_id)

    def should_have_default_priority(self):
        self.assertIsNone(self.properties.priority)

    def should_have_default_reply_to(self):
        self.assertIsNone(self.properties.reply_to)

    def should_get_epoch(self):
        self.ctx.time.assert_called_once_with()

    def should_truncate_epoch(self):
        self.ctx.int.assert_called_once_with(sentinel.epoch)

    def should_set_timestamp(self):
        self.assertIs(self.properties.timestamp, sentinel.timestamp)

    def should_have_default_type(self):
        self.assertIsNone(self.properties.type)

    def should_have_default_user_id(self):
        self.assertIsNone(self.properties.user_id)


class WhenCreatingPropertiesWithHeader(unittest.TestCase):
    kwargs = {
        'app_id': '3PO',
        'cluster_id': 'mfalcon',
        'content_encoding': 'GZIP',
        'content_type': 'application/JSON',
        'correlation_id': 'dockingbay94',
        'delivery_mode': 2,
        'expiration': '60000',  # 1 minute TTL
        'headers': {'tags': ['best practices']},
        'message_id': '1234',
        'priority': 9,
        'reply_to': 'reply_to_address',
        'timestamp': '1234567999',
        'type': 'message_type_name',
        'user_id': 'amqp_user_id',
    }

    def setUp(self):
        self.properties = Properties(header=BasicProperties(**self.kwargs))

    def should_set_app_id(self):
        self.assertEqual(self.properties.app_id, self.kwargs['app_id'])

    def should_set_cluster_id(self):
        self.assertEqual(self.properties.cluster_id, self.kwargs['cluster_id'])

    def should_set_content_encoding(self):
        self.assertEqual(self.properties.content_encoding,
                         self.kwargs['content_encoding'])

    def should_set_content_type(self):
        self.assertEqual(self.properties.content_type,
                         self.kwargs['content_type'])

    def should_set_correlation_id(self):
        self.assertEqual(self.properties.correlation_id,
                         self.kwargs['correlation_id'])

    def should_set_delivery_mode(self):
        self.assertEqual(self.properties.delivery_mode,
                         self.kwargs['delivery_mode'])

    def should_set_expiration(self):
        self.assertEqual(self.properties.expiration, self.kwargs['expiration'])

    def should_set_headers(self):
        self.assertEqual(self.properties.headers, self.kwargs['headers'])

    def should_set_message_id(self):
        self.assertEqual(self.properties.message_id, self.kwargs['message_id'])

    def should_set_priority(self):
        self.assertEqual(self.properties.priority, self.kwargs['priority'])

    def should_set_reply_to(self):
        self.assertEqual(self.properties.reply_to, self.kwargs['reply_to'])

    def should_set_timestamp(self):
        self.assertEqual(self.properties.timestamp, self.kwargs['timestamp'])

    def should_set_type(self):
        self.assertEqual(self.properties.type, self.kwargs['type'])

    def should_set_user_id(self):
        self.assertEqual(self.properties.user_id, self.kwargs['user_id'])
