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
