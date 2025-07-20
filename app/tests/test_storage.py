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

    def test_get_list_range_start_greater_then_end(self):
        self.storage.set("list", ["a", "b", "c", "d", "e"])

        self.assertEqual(self.storage.get_list_range("list", 2, 1), [])

    def test_get_list_range_negative_end(self):
        self.storage.set("list", ["a", "b", "c", "d", "e"])

        self.assertEqual(self.storage.get_list_range("list", 0, -3), ["a", "b", "c"])

    def test_get_list_negative_range(self):
        self.storage.set("list", ["a", "b", "c", "d", "e"])

        self.assertEqual(self.storage.get_list_range("list", -2, -1), ["d", "e"])

    def test_get_list_negative_range2(self):
        self.storage.set("pear", ["apple", "banana", "orange", "pear"])

        self.assertEqual(
            self.storage.get_list_range("pear", -2, -1), ["orange", "pear"]
        )

    def test_get_list_negative_range3(self):
        self.storage.set(
            "blueberry", ["raspberry", "grape", "orange", "mango", "pear", "banana"]
        )

        self.assertEqual(
            self.storage.get_list_range("blueberry", -7, -1),
            ["raspberry", "grape", "orange", "mango", "pear", "banana"],
        )

    def test_get_list_negative_range3(self):
        self.storage.set(
            "blueberry", ["raspberry", "grape", "orange", "mango", "pear", "banana"]
        )

        self.assertEqual(
            self.storage.get_list_range("blueberry", 0, 2),
            ["raspberry", "grape", "orange"],
        )
