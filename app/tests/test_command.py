import unittest

from app.command import ECHO, GET, LLEN, PING, SET

from app.storage import storage


class TestCommand(unittest.TestCase):

    def test_ping(self):
        self.assertEqual(PING(*[]).execute(), b"+PONG\r\n")

    def test_echo(self):
        self.assertEqual(ECHO(*["hello", "world!"]).execute(), b"+hello world!\r\n")

    def test_set(self):
        key, value = "foo", "bar"
        self.assertEqual(SET(key, value).execute(), b"+OK\r\n")
        self.assertEqual(storage.get(key), value)

    def test_set_zero_px_ttl(self):
        key, value = "foo", "bar"
        self.assertEqual(SET(key, value, "px", "0").execute(), b"+OK\r\n")
        self.assertIsNone(storage.get(key))

    def test_set_long_px_ttl(self):
        key, value = "foo", "bar"
        self.assertEqual(SET(key, value, "px", "10").execute(), b"+OK\r\n")
        self.assertEqual(storage.get(key), value)

    def test_get_exists(self):
        key, value = "foo", "bar"
        storage.set(key, value)
        self.assertEqual(GET(key).execute(), b"$3\r\nbar\r\n")

    def test_get_does_not_exist(self):
        key = "foo"
        self.assertEqual(GET(key).execute(), b"$-1\r\n")

    def test_llen_exists(self):
        key, values = "fruit", "apple banana strawberry".split()
        storage.set(key, values)
        self.assertEqual(LLEN(key).execute(), f":{len(values)}\r\n".encode())

    def test_llen_exists(self):
        key = "vegetables"
        self.assertEqual(LLEN(key).execute(), b":0\r\n")


if __name__ == "__main__":
    unittest.main()
