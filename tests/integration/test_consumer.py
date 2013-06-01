from mock import NonCallableMagicMock, sentinel

from pikachewie.consumer import Consumer
from pikachewie.message import Message
from tests import unittest


class WhenProcessingMessage(unittest.TestCase):

    class MockConsumer(Consumer):
        total_characters = 0

        def process_message(self):
            self.total_characters += len(self.message.payload)

    def setUp(self):
        headers = NonCallableMagicMock()
        body = "It's not wise to upset a Wookiee."
        self.message = Message(sentinel.channel, sentinel.method, headers,
                               body)
        self.consumer = self.MockConsumer()

        self.consumer.process(self.message)

    def should_set_message(self):
        self.assertIs(self.consumer.message, self.message)

    def should_perform_work(self):
        self.assertEqual(self.consumer.total_characters, 33)
