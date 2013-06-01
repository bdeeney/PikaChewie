from mock import MagicMock, sentinel

from pikachewie.consumer import Consumer
from tests import unittest


class DescribeMessage(unittest.TestCase):

    def should_require_subclasses_to_implement_process_message(self):
        self.assertRaises(NotImplementedError, Consumer().process_message)


class WhenProcessingMessage(unittest.TestCase):

    def setUp(self):
        self.consumer = Consumer()
        self.consumer.process_message = MagicMock()

        self.consumer.process(sentinel.message)

    def should_set_message(self):
        self.assertIs(self.consumer.message, sentinel.message)

    def should_call_process_message(self):
        self.consumer.process_message.assert_called_once_with()
