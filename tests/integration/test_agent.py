import time

import pyrabbit
from pika.adapters.tornado_connection import TornadoConnection
from pika.channel import Channel
from tornado.testing import AsyncTestCase
from testfixtures import LogCapture

from pikachewie.agent import ConsumerAgent
from pikachewie.broker import Broker
from pikachewie.consumer import Consumer
from pikachewie.utils import cached_property
from tests import _BaseTestCase, LoggingConsumer


class DescribeConsumerAgent(_BaseTestCase):

    def configure(self):
        self.consumer = Consumer()
        self.broker = Broker()
        self.bindings = [('myqueue', 'myexchange', 'my.routing.key')]

    def execute(self):
        self.agent = ConsumerAgent(self.consumer, self.broker, self.bindings)

    def should_have_consumer(self):
        self.assertIs(self.agent.consumer, self.consumer)

    def should_have_broker(self):
        self.assertIs(self.agent.broker, self.broker)

    def should_have_bindings(self):
        self.assertIs(self.agent.bindings, self.bindings)


class MockAgent(ConsumerAgent):

    def __init__(self, broker, consumer, bindings, no_ack=False, config=None,
                 stop_callback=None):
        super(MockAgent, self).__init__(broker, consumer, bindings, no_ack,
                                        config)
        self._stop = stop_callback

    def start_consuming(self, queue):
        super(MockAgent, self).start_consuming(queue)
        if self._stop:
            self._stop()

    def process(self, channel, method, header, body):
        super(MockAgent, self).process(channel, method, header, body)
        if self._stop:
            self._stop(body)


class AsyncAgentTestCase(AsyncTestCase):
    host = 'localhost'
    user = 'guest'
    virtual_host = '/integration'
    admin = pyrabbit.api.Client(host + ':15672', user, 'guest')
    broker = Broker({'rabbitmq': {'host': host}},
                    {'virtual_host': virtual_host})
    consumer = LoggingConsumer()
    bindings = (
        {
            'queue': 'my.queue',
            'exchange': 'my.exchange',
            'routing_key': 'my.routing.key',
        },
    )
    config = {
        'exchanges': {'my.exchange': {'exchange_type': 'topic',
                                      'durable': True}},
        'queues': {'my.queue': {'durable': True,
                                'arguments': {'x-ha-policy': 'all'}}},
    }
    payload = "It's not wise to upset a Wookie."

    def get_new_ioloop(self):
        return self.agent.connection.ioloop

    @cached_property
    def agent(self):
        agent = MockAgent(self.consumer, self.broker, self.bindings, False,
                          self.config, stop_callback=self.stop)
        agent.connect()
        return agent

    @property
    def escaped_vhost(self):
        return self.virtual_host.replace('/', '%2F')

    def setUp(self):
        self._cleanup_virtual_host()
        self._init_virtual_host()
        super(AsyncAgentTestCase, self).setUp()
        self.wait()

    def tearDown(self):
        super(AsyncAgentTestCase, self).tearDown()
        self._cleanup_virtual_host()
        del self.agent

    def _cleanup_virtual_host(self):
        if self.virtual_host in self.admin.get_vhost_names():
            self.admin.delete_vhost(self.escaped_vhost)

    def _init_virtual_host(self):
        self.admin.create_vhost(self.escaped_vhost)
        self.admin.set_vhost_permissions(self.escaped_vhost, self.user,
                                         config='.*', rd='.*', wr='.*')

    def _poll_queue_statistics(self, queue, callback):
        attempts = 10
        for i in range(attempts):
            queue_stats = self.admin.get_queue(self.escaped_vhost, queue)
            if callback(queue_stats):
                return queue_stats
            time.sleep(1)

        raise AssertionError('Callback condition not satisfied after %d '
                             'attempts in _poll_queue_statistics' % attempts)


class WhenConnectingToBroker(AsyncAgentTestCase):

    def setUp(self):
        super(WhenConnectingToBroker, self).setUp()
        assert self.admin.publish(self.escaped_vhost, 'my.exchange',
                                  'my.routing.key', self.payload)
        with LogCapture('tests.LoggingConsumer') as self.consumer_logs:
            self.message_body = self.wait()

    def should_set_connection(self):
        self.assertIsInstance(self.agent.connection, TornadoConnection)

    def should_set_channel(self):
        self.assertIsInstance(self.agent.channel, Channel)

    def should_create_exchange(self):
        self.assertIsNotNone(self.get_exchange())

    def should_set_exchange_type(self):
        self.assertEqual(self.get_exchange()['type'], 'topic')

    def should_set_exchange_parameters(self):
        self.assertTrue(self.get_exchange()['durable'])

    def should_create_queue(self):
        self.assertIsNotNone(self.get_queue())

    def should_set_queue_parameters(self):
        self.assertTrue(self.get_queue()['durable'])

    def should_set_queue_arguments(self):
        self.assertEqual(self.get_queue()['arguments'],
                         {'x-ha-policy': 'all'})

    def should_create_queue_binding(self):
        expected = self.bindings[0]
        bindings = self.admin.get_queue_bindings(self.escaped_vhost,
                                                 'my.queue')
        for binding in bindings:
            if binding['routing_key'] == expected['routing_key'] and \
                    binding['source'] == expected['exchange']:
                # expected queue binding exists, thus the test is successful
                return

        raise AssertionError('Expected queue binding does not exist: %s' %
                             expected)

    def should_start_consuming_from_queue(self):
        has_consumers = lambda queue_stats: 'consumers' in queue_stats and \
                                            queue_stats['consumers'] > 0
        queue_stats = self._poll_queue_statistics('my.queue', has_consumers)
        self.assertEqual(queue_stats['consumers'], 1)
        self.assertEqual(queue_stats['consumer_details'][0]['consumer_tag'],
                         self.agent._consumer_tags['my.queue'])

    def should_consume_message(self):
        self.assertEqual(self.message_body, self.payload)

    def should_dispatch_message_processing_to_consumer(self):
        self.consumer_logs.check(('tests.LoggingConsumer', 'INFO',
                                  'Processed message: ' + self.payload))

    def get_exchange(self):
        return self.admin.get_exchange(self.escaped_vhost, 'my.exchange')

    def get_queue(self):
        return self.admin.get_queue(self.escaped_vhost, 'my.queue')
