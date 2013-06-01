"""
=======================================================
pikachewie.exceptions -- PikaChewie consumer exceptions
=======================================================

This module was forked from Gavin M. Roy's :mod:`rejected.exceptions`.

ref: https://github.com/gmr/rejected/blob/master/LICENSE

"""


class ConsumerException(Exception):
    """May be called when processing a message to indicate a problem that the
    Consumer may be experiencing that should cause it to stop.

    """
    pass


class MessageException(Exception):
    """Invoke when a message should be rejected and not requeued, but not due
    to a processing error that should cause the Consumer to stop.

    """
    pass
