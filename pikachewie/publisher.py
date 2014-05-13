"""
=======================================================
pikachewie.publisher -- PikaChewie base publisher class
=======================================================

"""
import logging
import time

import simplejson
from pika.exceptions import ConnectionClosed, ChannelClosed
from pika.spec import BasicProperties

log = logging.getLogger(__name__)


class PublisherMixin(object):
    """Mixin for publishing messages to RabbitMQ."""

    def publish(self, exchange, routing_key, body, properties=None):
        """Publish a message to RabbitMQ.

        `properties` should be an instance of pikachewie.data.Properties or
        None.

        :param str exchange: the exchange to publish to
        :param str routing_key: the routing key to publish with
        :param str|unicode body: the message body to publish
        :param pikachewie.data.Properties properties: the message properties

        """
        if properties:
            properties = self._build_basic_properties(properties)
        try:
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                body=body,
            )
        except (ConnectionClosed, ChannelClosed) as exc:
            log.warn('Cannot publish on existing channel')
            log.info('Attempting to republish on new channel')
            self._channel = None
            self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                properties=properties,
                body=body,
            )

    def _build_basic_properties(self, properties):
        """
        Get the pika.BasicProperties from a pikachewie.data.Properties object.

        :param pikachewie.data.Properties properties: properties to convert
        :rtype: pika.spec.BasicProperties

        """
        basic_properties = BasicProperties()
        attrs = '''
            app_id
            content_encoding
            content_type
            correlation_id
            delivery_mode
            priority
            reply_to
            message_id
            type
            user_id
        '''.split()
        for attr in attrs:
            value = getattr(properties, attr)
            if value is not None:
                setattr(basic_properties, attr, value)

        basic_properties.timestamp = properties.timestamp or int(time.time())
        if properties.expiration is not None:
            basic_properties.expiration = str(properties.expiration)
        if properties.headers is not None and \
                len(properties.headers.keys()):
            basic_properties.headers = dict(properties.headers)

        return basic_properties


class BlockingPublisher(PublisherMixin, object):
    """Base class for synchronous RabbitMQ publishers."""
    _channel = None

    def __init__(self, broker):
        self.broker = broker

    @property
    def channel(self):
        """Return an open channel to the RabbitMQ broker.

        If necessary, create and cache a new channel.

        """
        if not self._channel or not self._channel.is_open:
            self._channel = self.broker.connect(blocking=True).channel()
            self._channel.confirm_delivery()
        return self._channel


class JSONPublisherMixin(PublisherMixin):
    """Publisher Mixin that JSON-serializes the message payload."""
    def _serialize(self, value):
        """Serialize the inbound value as JSON.

        :param dict|list|str|number value: The value to serialize
        :return: str

        """
        return simplejson.dumps(value, ensure_ascii=False)

    def publish(self, exchange, routing_key, payload, properties=None):
        """Publish a message to RabbitMQ on the same channel the original
        message was received on, automatically serializing the message payload.

        :param str exchange: the exchange to publish to
        :param str routing_key: the routing key to publish with
        :param pikachewie.data.Properties: the message properties
        :param dict|list: the message body to publish

        """
        if not properties:
            properties = BasicProperties()
        properties.content_type = 'application/json'
        super(JSONPublisherMixin, self).publish(
            exchange,
            routing_key,
            self._serialize(payload),
            properties,
        )


class BlockingJSONPublisher(JSONPublisherMixin, BlockingPublisher):
    pass
