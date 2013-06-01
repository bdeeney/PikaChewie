import logging

from pikachewie.utils import import_namespaced_class
from tests import unittest


class TestImportNamspacedClass(unittest.TestCase):

    def test_import_namespaced_class(self):
        self.assertIs(import_namespaced_class('logging.Logger'),
                      logging.Logger)

    def test_import_namespaced_class_failure(self):
        self.assertRaises(ImportError, import_namespaced_class,
                          'pikachewie.no_such_module.Classname')
