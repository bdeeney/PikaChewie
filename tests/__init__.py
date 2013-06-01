"""
=========================================
pikachewie.tests -- PikaChewie test suite
=========================================

"""
import sys
import logging

from contextlib2 import ExitStack

from pikachewie.consumer import Consumer

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest


class _BaseTestCase(unittest.TestCase):
    __contexts__ = ()

    def context(self):
        stack = ExitStack()
        for (name, context) in self.__contexts__:
            setattr(stack, name, stack.enter_context(context))
        return stack

    def setUp(self):
        super(_BaseTestCase, self).setUp()
        with self.context() as self.ctx:
            self.configure()
            self.execute()

    def configure(self):
        pass

    def execute(self):
        pass


class LoggingConsumer(Consumer):
    """A :class:`pikachewie.consumer.Consumer` subclass for use in testing."""
    def __init__(self, level='info'):
        self.level = level
        log = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.log = getattr(log, self.level)

    def process_message(self):
        """Log the message body."""
        self.log('Processed message: %s', self.message.body)
