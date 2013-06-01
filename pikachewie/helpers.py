"""
=============================================================
pikachewie.helpers -- Helpers for creating brokers and agents
=============================================================

"""
from pikachewie.agent import ConsumerAgent
from pikachewie.broker import Broker
from pikachewie.utils import import_namespaced_class


def consumer_from_config(config):
    """
    Create a :class:`pikachewie.consumer.Consumer` from the given `config`.

    """
    consumer_class = import_namespaced_class(config['class'])
    kwargs = config.get('arguments', {})
    return consumer_class(**kwargs)


def broker_from_config(config):
    """Create a :class:`pikachewie.broker.Broker` from the given `config`."""
    options = config.copy()
    nodes = options.pop('nodes')
    return Broker(nodes, options)


def consumer_agent_from_config(config, name, broker='default',
                               section='rabbitmq'):
    """
    Create a :class:`pikachewie.agent.ConsumerAgent` from the given `config`.

    The `config` dict should have a structure similar to the following
    example::

        {
            'rabbitmq': {
                'brokers': {
                    'default': {
                        'nodes': {
                            'rabbit1': {
                                'host': 'rabbit1.example.com',
                                'port': 5672,
                            },
                            'rabbit2': {
                                'host': 'rabbit2.example.com',
                                'port': 5672,
                            },
                        },
                        'virtual_host': '/integration',
                        'heartbeat_interval': 60,
                    },
                },
                'consumers': {
                    'message_logger': {
                        'class': 'my.consumers.LoggingConsumer',
                        'arguments': {
                            'level': 'debug',
                        },
                        'bindings': [
                            {
                                'exchange': 'message',
                                'queue': 'text',
                                'routing_key': 'example.text.#',
                            },
                        ],
                    },
                },
                'exchanges': {
                    'message': {
                        'exchange_type': 'topic',
                        'durable': True,
                        'auto_delete': False,
                    },
                },
                'queues': {
                    'text': {
                        'durable': True,
                        'exclusive': False,
                        'arguments': {
                            'x-dead-letter-exchange': 'dead.letters',
                            'x-dead-letter-routing-key': 'omg.such.rejection',
                            'x-ha-policy': 'all',
                            'x-message-ttl': 1800000
                        },
                    },
                },
            },
        }

    """
    consumer = consumer_from_config(config[section]['consumers'][name])
    broker = broker_from_config(config[section]['brokers'][broker])
    bindings = config[section]['consumers'][name]['bindings']

    return ConsumerAgent(consumer, broker, bindings, config['rabbitmq'])
