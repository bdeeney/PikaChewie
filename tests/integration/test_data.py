import uuid

from mock import patch

from pikachewie.data import Properties
from tests import _BaseTestCase

mod = 'pikachewie.data'

_uuid = '8aa2d9ec-d53a-4596-91bf-2be4cf1e2652'


class DescribeProperties(_BaseTestCase):
    __contexts__ = (
        ('uuid4', patch(mod + '.uuid.uuid4',
                        return_value=uuid.UUID(_uuid))),
        ('time', patch(mod + '.time.time', return_value=1234567890.987654)),
    )

    def execute(self):
        self.properties = Properties()

    def should_create_message_id(self):
        self.assertEqual(self.properties.message_id, _uuid)

    def should_create_timestamp(self):
        self.assertEqual(self.properties.timestamp, 1234567890)
