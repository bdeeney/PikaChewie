"""
=====================================================
pikachewie.consumer -- PikaChewie base consumer class
=====================================================

"""
import logging

log = logging.getLogger(__name__)


class Consumer(object):
    """Base class for RabbitMQ consumers."""
    message = None

    def process(self, message):
        """Process the given RabbitMQ `message`.

        To implement logic for processing messages in `Consumer` subclasses,
        override :meth:`process_message`, not this method.

        :param message: the message to process
        :type message: :class:`pikachewie.message.Message`

        """
        log.debug('Received: %r', message)
        self.message = message
        log.debug('Calling %r', self.process_message)
        self.process_message()

    def process_message(self):
        """Subclasses must override this method to implement consumer logic."""
        raise NotImplementedError
