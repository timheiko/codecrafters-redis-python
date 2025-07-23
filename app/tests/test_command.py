import unittest

from app.command import ECHO, PING


class TestCommand(unittest.TestCase):

    def test_ping(self):
        self.assertEqual(PING(*[]).execute(), b"+PONG\r\n")

    def test_echo(self):
        self.assertEqual(ECHO(*["hello", "world!"]).execute(), b"+hello world!\r\n")


if __name__ == "__main__":
    unittest.main()
