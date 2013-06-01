"""
========================================
pikachewie.message -- A RabbitMQ message
========================================

"""
import bz2
import copy
import zlib
from datetime import datetime, timedelta

import simplejson

from pikachewie.data import DataObject, Properties
from pikachewie.utils import cached_property, delegate


class Message(DataObject):
    """A RabbitMQ message."""

    def __init__(self, channel, method, header, body):
        """
        Configure the message with the given channel, method, header, and body.

        :param channel: AMQP channel on which the message was received
        :type channel: :class:`pika.channel.Channel`

        :param method: AMQP method
        :type method: :class:`pika.amqp_object.Method`

        :param header: AMQP message properties (AMQP Basic.Properties)
        :type header: :class:`pika.spec.BasicProperties`

        :param body: message body
        :type body: str

        """
        self.channel = channel
        self.method = method
        self.properties = Properties(header)
        self.body = copy.copy(body)

    # AMQP method delegates
    consumer_tag = delegate('method', 'consumer_tag')
    delivery_tag = delegate('method', 'delivery_tag')
    exchange = delegate('method', 'exchange')
    redelivered = delegate('method', 'redelivered')
    routing_key = delegate('method', 'routing_key')

    # header properties delegates
    app_id = delegate('properties', 'app_id')
    correlation_id = delegate('properties', 'correlation_id')
    headers = delegate('properties', 'headers')
    id = delegate('properties', 'message_id')
    priority = delegate('properties', 'priority')
    reply_to = delegate('properties', 'reply_to')
    type = delegate('properties', 'type')
    user_id = delegate('properties', 'user_id')

    @cached_property
    def content_encoding(self):
        """Return the content encoding as a lowercase string.

        :rtype: :class:`str` or `NoneType`

        """
        return (self.properties.content_encoding or '').lower() or None

    @cached_property
    def content_type(self):
        """Return the content-type as a lowercase string.

        :rtype: :class:`str` or `NoneType`

        """
        return (self.properties.content_type or '').lower() or None

    @cached_property
    def created_at(self):
        """Return the message creation time as a `datetime.datetime`.

        :rtype: :class:`datetime.datetime`

        """
        if not self.properties.timestamp:
            return None
        return datetime.fromtimestamp(float(self.properties.timestamp))

    @cached_property
    def expires_at(self):
        """Return the message expiration time as a `datetime.datetime`.

        :rtype: :class:`datetime.datetime`

        """
        if not (self.created_at and self.properties.expiration):
            return None
        ttl = timedelta(milliseconds=int(self.properties.expiration))
        return self.created_at + ttl

    @property
    def is_expired(self):
        """Whether this message has expired.

        :rtype: :class:`bool`

        """
        return self.expires_at is not None and \
            self.expires_at <= datetime.now()

    @cached_property
    def payload(self):
        """Return the decoded, deserialized contents of the message body.

        :rtype: any

        """
        payload = self._decoded_body

        # Deserialize JSON message bodies
        if self.content_type == 'application/json':
            payload = simplejson.loads(payload, use_decimal=True)

        return payload

    @cached_property
    def _decoded_body(self):
        """Return the decoded message body as a string.

        :rtype: :class:`str`
        """
        result = self.body

        # Handle bzip2 compressed content
        if self.content_encoding == 'bzip2':
            result = bz2.decompress(result)
        # Handle zlib compressed content
        elif self.content_encoding == 'gzip':
            result = zlib.decompress(result)

        return result
