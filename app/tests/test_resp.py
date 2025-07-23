import unittest
from ..resp import decode_bulk_string, encode, encode_simple


class RespTest(unittest.TestCase):

    def test_encode_bulk_string_ok(self):
        self.assertEqual(encode("OK"), b"$2\r\nOK\r\n")

    def test_encode_bulk_string_pong(self):
        self.assertEqual(encode("pong"), b"$4\r\npong\r\n")

    def test_encode_bulk_string_empty(self):
        self.assertEqual(encode(""), b"$0\r\n\r\n")

    def test_encode_simple_string_pong(self):
        self.assertEqual(encode_simple("pong"), b"+pong\r\n")

    def test_encode_int(self):
        self.assertEqual(encode(10), b":10\r\n")

    def test_encode_null(self):
        self.assertEqual(encode(None), b"$-1\r\n")

    def test_encode_array(self):
        self.assertEqual(
            encode(["foo", None, "bar"]), b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n"
        )

    def test_encode_array_empty(self):
        self.assertEqual(encode([]), b"*0\r\n")

    def test_decode_bulk_string(self):
        self.assertEqual(decode_bulk_string(b"$6\r\nfoobar\r\n", 0), ("foobar", 12))

    def test_decode_bulk_string_empty(self):
        self.assertEqual(decode_bulk_string(b"$0\r\n\r\n", 0), ("", 6))


if __name__ == "__main__":
    unittest.main()
