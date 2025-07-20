import unittest

from app.storage import Storage


class TestStorage(unittest.TestCase):

    def setUp(self):
        self.storage = Storage()

    def test_missing_key(self):
        self.assertIsNone(self.storage.get("unknown_key"))

    def test_existing_key(self):
        self.storage.set("answer", 42)

        self.assertEqual(self.storage.get("answer"), 42)

    def test_set_get_expired(self):
        self.storage.set("answer", 42, 0)

        self.assertIsNone(self.storage.get("answer"))

    def test_get_list_range(self):
        self.storage.set("list", [1, "2", 3])

        self.assertEqual(self.storage.get_list_range("list", 0, 1), [1, "2"])

    def test_get_empty_list_range(self):
        self.storage.set("list", [])

        self.assertEqual(self.storage.get_list_range("list", 0, 1), [])

    def test_get_missing_list_range(self):
        self.assertEqual(self.storage.get_list_range("list", 0, 1), [])

    def test_get_list_range(self):
        self.storage.set("list", [1, "2", 3])

        self.assertEqual(self.storage.get_list_range("list", 3, 2), [])
