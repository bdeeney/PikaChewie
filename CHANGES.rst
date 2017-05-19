CHANGES
=======

1.3 2017-05-19
--------------

- Add ``retry_on_exceptions`` and ``process_data_events()`` to
  ``PublisherMixin`` (#5 by @the-allanc).

1.2 2017-05-15
--------------

- Add deleter for ``BlockingPublisher.channel`` (#4 by @the-allanc).
- Configure Travis CI.

1.1.1 2016-12-12
----------------

- Unpin pika dependency now that pika 0.10.0 has been released.

1.1 2015-08-19
--------------

- Pin to pika 0.10.0b2.
- Add Python 3 support.

1.0.1 2015-08-17
----------------

- Bump to pika 0.9.14.

1.0 2014-05-13
--------------

- Support `no_ack` parameter in ``ConsumerAgent``.

0.10.0 2014-05-13
-----------------

- Initial release.