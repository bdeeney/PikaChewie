from datetime import datetime, timedelta

from mock import MagicMock, NonCallableMagicMock, PropertyMock, patch, sentinel

from pikachewie.message import Message
from tests import _BaseTestCase


mod = 'pikachewie.message'
sut = mod + '.Message'


class WhenCreatingMessage(_BaseTestCase):
    __contexts__ = (
        ('Properties', patch(mod + '.Properties',
                             return_value=sentinel.properties)),
        ('copy', patch(mod + '.copy')),
    )

    def configure(self):
        self.ctx.copy.copy.return_value = sentinel.body_copy
        self.header = NonCallableMagicMock()

    def execute(self):
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def should_set_channel(self):
        self.assertIs(self.message.channel, sentinel.channel)

    def should_set_method(self):
        self.assertIs(self.message.method, sentinel.method)

    def should_create_properties(self):
        self.ctx.Properties.assert_called_once_with(self.header)

    def should_set_properties(self):
        self.assertIs(self.message.properties, sentinel.properties)

    def should_copy_body(self):
        self.ctx.copy.copy.assert_called_once_with(sentinel.body)

    def should_set_body(self):
        self.assertIs(self.message.body, sentinel.body_copy)


class DescribeMessage(_BaseTestCase):

    def configure(self):
        self.method = NonCallableMagicMock()
        self.header = NonCallableMagicMock()

    def execute(self):
        self.message = Message(sentinel.channel, self.method, self.header,
                               sentinel.body)

    def should_delegate_consumer_tag(self):
        self.assertIs(self.message.consumer_tag,
                      self.message.method.consumer_tag)

    def should_delegate_delivery_tag(self):
        self.assertIs(self.message.delivery_tag,
                      self.message.method.delivery_tag)

    def should_delegate_exchange(self):
        self.assertIs(self.message.exchange, self.message.method.exchange)

    def should_delegate_redelivered(self):
        self.assertIs(self.message.redelivered,
                      self.message.method.redelivered)

    def should_delegate_routing_key(self):
        self.assertIs(self.message.routing_key,
                      self.message.method.routing_key)

    def should_delegate_app_id(self):
        self.assertIs(self.message.app_id, self.message.properties.app_id)

    def should_delegate_correlation_id(self):
        self.assertIs(self.message.correlation_id,
                      self.message.properties.correlation_id)

    def should_delegate_headers(self):
        self.assertIs(self.message.headers, self.message.properties.headers)

    def should_delegate_id(self):
        self.assertIs(self.message.id, self.message.properties.message_id)

    def should_delegate_priority(self):
        self.assertIs(self.message.priority, self.message.properties.priority)

    def should_delegate_reply_to(self):
        self.assertIs(self.message.reply_to, self.message.properties.reply_to)

    def should_delegate_type(self):
        self.assertIs(self.message.type, self.message.properties.type)

    def should_delegate_user_id(self):
        self.assertIs(self.message.user_id, self.message.properties.user_id)


class WhenGettingUndefinedMessageContentEncoding(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock(content_encoding=None)
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.content_encoding = self.message.content_encoding

    def should_lower_content_encoding(self):
        self.assertIsNone(self.message.content_encoding)


class WhenGettingMessageContentEncoding(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock(content_encoding='GZIP')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.created_at = self.message.created_at

    def should_lower_content_encoding(self):
        self.assertEqual(self.message.content_encoding, 'gzip')


class WhenGettingUndefinedMessageContentType(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock(content_type=None)
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.content_type = self.message.content_type

    def should_lower_content_type(self):
        self.assertIsNone(self.message.content_type)


class WhenGettingMessageContentType(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock(content_type='application/JSON')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.created_at = self.message.created_at

    def should_lower_content_type(self):
        self.assertEqual(self.message.content_type, 'application/json')


class WhenGettingMessageCreatedAt(_BaseTestCase):
    __contexts__ = (
        ('float', patch(mod + '.float', create=True,
                        return_value=sentinel.epoch)),
        ('datetime', patch(mod + '.datetime')),
    )

    def configure(self):
        self.ctx.datetime.fromtimestamp.return_value = sentinel.created_at
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.created_at = self.message.created_at

    def should_convert_timestamp_to_float(self):
        self.ctx.float.assert_called_once_with(self.header.timestamp)

    def should_convert_epoch_to_datetime(self):
        self.ctx.datetime.fromtimestamp.assert_called_once_with(sentinel.epoch)

    def should_return_created_at(self):
        self.assertIs(self.created_at, sentinel.created_at)


class WhenMessageCreatedAtUndefined(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock(timestamp=None)
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.created_at = self.message.created_at

    def should_return_none(self):
        self.assertIsNone(self.created_at)


class WhenGettingMessageExpiresAt(_BaseTestCase):
    milliseconds = 60000
    __contexts__ = (
        ('int', patch(mod + '.int', create=True,
                      return_value=milliseconds)),
        ('timedelta', patch(mod + '.timedelta',
                            return_value=timedelta(milliseconds=milliseconds))
         ),
        ('created_at', patch(sut + '.created_at', new_callable=PropertyMock,
                             return_value=datetime(2009, 2, 13, 18, 31, 30))),
    )

    def configure(self):
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.expires_at = self.message.expires_at

    def should_convert_expiration_to_int(self):
        self.ctx.int.assert_called_once_with(self.header.expiration)

    def should_convert_milliseconds_to_timedelta(self):
        self.ctx.timedelta.assert_called_once_with(
            milliseconds=self.milliseconds)

    def should_return_expires_at(self):
        self.assertEqual(self.message.expires_at,
                         datetime(2009, 2, 13, 18, 32, 30))


class WhenMessageExpiresAtUndefinedDueToExpiration(_BaseTestCase):

    def configure(self):
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)
        self.message.properties = NonCallableMagicMock(expiration=None)

    def execute(self):
        self.expires_at = self.message.expires_at

    def should_return_none(self):
        self.assertIsNone(self.expires_at)


class WhenMessageExpiresAtUndefined(_BaseTestCase):
    __contexts__ = (
        ('expires_at', patch(sut + '.expires_at', new_callable=PropertyMock,
                             return_value=None)),
    )

    def configure(self):
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_never_expire(self):
        self.assertIs(self.is_expired, False)


class WhenMessageHasNotExpired(_BaseTestCase):
    __contexts__ = (
        ('datetime', patch(mod + '.datetime')),
        ('expires_at', patch(sut + '.expires_at', new_callable=PropertyMock,
                             return_value=datetime(2009, 2, 13, 18, 32, 30))),
    )

    def configure(self):
        self.ctx.datetime.now.return_value = datetime(2009, 2, 13, 18, 32, 29)
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_not_be_expired(self):
        self.assertIs(self.is_expired, False)


class WhenMessageHasExpired(_BaseTestCase):
    __contexts__ = (
        ('datetime', patch(mod + '.datetime')),
        ('expires_at', patch(sut + '.expires_at', new_callable=PropertyMock,
                             return_value=datetime(2009, 2, 13, 18, 32, 30))),
    )

    def configure(self):
        self.ctx.datetime.now.return_value = datetime(2009, 2, 13, 18, 32, 30)
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.is_expired = self.message.is_expired

    def should_not_be_expired(self):
        self.assertIs(self.is_expired, True)


class WhenGettingPayload(_BaseTestCase):
    __contexts__ = (
        ('_decoded_body', patch(sut + '._decoded_body',
                                new_callable=PropertyMock,
                                return_value=sentinel.decoded_body)),
    )

    def configure(self):
        self.header = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.payload = self.message.payload

    def should_decode_body(self):
        self.ctx._decoded_body.assert_called_once_with()

    def should_return_decoded_body(self):
        self.assertEqual(self.payload, sentinel.decoded_body)


class WhenDecodingBzippedBody(_BaseTestCase):
    __contexts__ = (
        ('bz2', patch(mod + '.bz2',
                      decompress=MagicMock(
                      return_value=sentinel.decompressed_body))),
    )

    def configure(self):
        self.header = NonCallableMagicMock(content_encoding='bzip2')
        self.body = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               self.body)

    def execute(self):
        self._decoded_body = self.message._decoded_body

    def should_decompress_body(self):
        self.ctx.bz2.decompress.assert_called_once_with(self.body)

    def should_return_decompressed_body(self):
        self.assertEqual(self._decoded_body, sentinel.decompressed_body)


class WhenDecodingGzippedBody(_BaseTestCase):
    __contexts__ = (
        ('zlib', patch(mod + '.zlib',
                       decompress=MagicMock(
                       return_value=sentinel.decompressed_body))),
    )

    def configure(self):
        self.header = NonCallableMagicMock(content_encoding='gzip')
        self.body = NonCallableMagicMock()
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               self.body)

    def execute(self):
        self._decoded_body = self.message._decoded_body

    def should_decompress_body(self):
        self.ctx.zlib.decompress.assert_called_once_with(self.body)

    def should_return_decompressed_body(self):
        self.assertEqual(self._decoded_body, sentinel.decompressed_body)


class WhenGettingJsonPayload(_BaseTestCase):
    __contexts__ = (
        ('_decoded_body', patch(sut + '._decoded_body',
                                new_callable=PropertyMock,
                                return_value=sentinel.decoded_body)),
        ('loads', patch(mod + '.simplejson.loads',
                        return_value=sentinel.deserialized_body))
    )

    def configure(self):
        self.header = NonCallableMagicMock(content_type='application/json')
        self.message = Message(sentinel.channel, sentinel.method, self.header,
                               sentinel.body)

    def execute(self):
        self.payload = self.message.payload

    def should_decode_body(self):
        self.ctx._decoded_body.assert_called_once_with()

    def should_deserialize_decoded_body(self):
        self.ctx.loads.assert_called_once_with(sentinel.decoded_body,
                                               use_decimal=True)

    def should_return_deserialized_body(self):
        self.assertEqual(self.payload, sentinel.deserialized_body)
