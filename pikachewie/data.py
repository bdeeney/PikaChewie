"""
==========================================
pikachewie.data -- PikaChewie data objects
==========================================

This module was forked from Gavin M. Roy's :mod:`rejected.data`.

ref: https://github.com/gmr/rejected/blob/master/LICENSE

"""
import copy
import time
import uuid

from pika.spec import BasicProperties


class DataObject(object):
    """Mixin that adds an object's attributes to its representation."""
    def __repr__(self):
        """Return a string representation of the object and its attributes.

        :rtype: str

        """
        items = list()
        for key, value in self.__dict__.iteritems():
            if getattr(self.__class__, key, None) != value:
                items.append('%s=%s' % (key, value))
        return "<%s(%s)>" % (self.__class__.__name__, items)


class Properties(DataObject):
    """Class encapsulating attributes defined in AMQP's Basic.Properties."""
    attrs = '''
        app_id
        cluster_id
        content_type
        content_encoding
        correlation_id
        delivery_mode
        expiration
        message_id
        priority
        reply_to
        timestamp
        type
        user_id
    '''.split()

    def __init__(self, header=None):
        """Configure this object's attributes using the given header.

        :param header: Pika header object
        :type header: :class:`pika.spec.BasicProperties`

        """
        if header is None:
            header = BasicProperties(content_type='text/text',
                                     delivery_mode=1,
                                     message_id=str(uuid.uuid4()),
                                     timestamp=int(time.time()))
        for attr in self.attrs:
            setattr(self, attr, getattr(header, attr))
        self.headers = copy.deepcopy(header.headers) or {}
