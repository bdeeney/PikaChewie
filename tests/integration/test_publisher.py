import pyrabbit
from simplejson import dumps

from pikachewie.broker import Broker
from pikachewie.publisher import BlockingPublisher, BlockingJSONPublisher
from tests import _BaseTestCase


class WhenPublishingMessage(_BaseTestCase):
    host = 'localhost'
    user = 'guest'
    virtual_host = '/integration'
    admin = pyrabbit.api.Client(host + ':15672', user, 'guest')
    broker = Broker({'rabbitmq': {'host': host}},
                    {'virtual_host': virtual_host})
    exchange = 'my.exchange'
    routing_key = 'my.routing.key'
    queue = 'my.queue'
    payload = "It's not wise to upset a Wookie."

    @property
    def escaped_vhost(self):
        return self.virtual_host.replace('/', '%2F')

    def configure(self):
        self._cleanup_virtual_host()
        self._init_virtual_host()

    def execute(self):
        BlockingPublisher(self.broker).publish(self.exchange, self.routing_key,
                                               self.payload)

    def tearDown(self):
        super(WhenPublishingMessage, self).tearDown()
        self._cleanup_virtual_host()

    def _cleanup_virtual_host(self):
        if self.virtual_host in self.admin.get_vhost_names():
            self.admin.delete_vhost(self.escaped_vhost)

    def _init_virtual_host(self):
        self.admin.create_vhost(self.escaped_vhost)
        self.admin.set_vhost_permissions(self.escaped_vhost, self.user,
                                         config='.*', rd='.*', wr='.*')
        self.admin.create_exchange(self.escaped_vhost, self.exchange, 'direct')
        self.admin.create_queue(self.escaped_vhost, self.queue)
        self.admin.create_binding(self.escaped_vhost, self.exchange,
                                  self.queue, self.routing_key)

    def should_publish_message(self):
        messages = self.admin.get_messages(self.escaped_vhost, self.queue)
        expected = [{
            'exchange': self.exchange,
            'routing_key': self.routing_key,
            'payload': self.payload,
            'payload_bytes': len(self.payload),
            'message_count': 0,
            'payload_encoding': 'string',
            'redelivered': False,
            'properties': [],
        }]
        self.assertEqual(messages, expected)


class WhenPublishingJSONMessage(_BaseTestCase):
    host = 'localhost'
    user = 'guest'
    virtual_host = '/integration'
    admin = pyrabbit.api.Client(host + ':15672', user, 'guest')
    broker = Broker({'rabbitmq': {'host': host}},
                    {'virtual_host': virtual_host})
    exchange = 'my.exchange'
    routing_key = 'my.routing.key'
    queue = 'my.queue'
    payload = {'advice': "It's not wise to upset a Wookie."}

    @property
    def escaped_vhost(self):
        return self.virtual_host.replace('/', '%2F')

    def configure(self):
        self._cleanup_virtual_host()
        self._init_virtual_host()

    def execute(self):
        BlockingJSONPublisher(self.broker).publish(self.exchange,
                                                   self.routing_key,
                                                   self.payload)

    def tearDown(self):
        super(WhenPublishingJSONMessage, self).tearDown()
        self._cleanup_virtual_host()

    def _cleanup_virtual_host(self):
        if self.virtual_host in self.admin.get_vhost_names():
            self.admin.delete_vhost(self.escaped_vhost)

    def _init_virtual_host(self):
        self.admin.create_vhost(self.escaped_vhost)
        self.admin.set_vhost_permissions(self.escaped_vhost, self.user,
                                         config='.*', rd='.*', wr='.*')
        self.admin.create_exchange(self.escaped_vhost, self.exchange, 'direct')
        self.admin.create_queue(self.escaped_vhost, self.queue)
        self.admin.create_binding(self.escaped_vhost, self.exchange,
                                  self.queue, self.routing_key)

    def should_publish_message(self):
        messages = self.admin.get_messages(self.escaped_vhost, self.queue)
        expected = [{
            'exchange': self.exchange,
            'routing_key': self.routing_key,
            'payload': dumps(self.payload),
            'payload_bytes': len(dumps(self.payload)),
            'message_count': 0,
            'payload_encoding': 'string',
            'redelivered': False,
            'properties': {
                'content_type': 'application/json',
            },
        }]
        expected[0]['properties']['timestamp'] = \
            messages[0]['properties']['timestamp']
        self.assertEqual(messages, expected)
