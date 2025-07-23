import unittest

from app.command import PING


class TestCommand(unittest.TestCase):

    def test_pint(self):
        self.assertEqual(PING(*[]).execute(), b"+PONG\r\n")


if __name__ == "__main__":
    unittest.main()
