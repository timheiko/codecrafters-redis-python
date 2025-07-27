import unittest
from ..resp import decode, decode_bulk_string, decode_commands, encode, encode_simple


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

    def test_encode_error(self):
        self.assertEqual(encode(ValueError("Bang!")), b"-ERR Bang!\r\n")

    def test_decode_bulk_string(self):
        self.assertEqual(decode_bulk_string(b"$6\r\nfoobar\r\n", 0), ("foobar", 12))

    def test_decode_bulk_string_empty(self):
        self.assertEqual(decode_bulk_string(b"$0\r\n\r\n", 0), ("", 6))

    def test_decode_list(self):
        self.assertEqual(
            decode(
                b"*7\r\n$5\r\nRPUSH\r\n$9\r\nlist_key2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
            ),
            ["RPUSH", "list_key2", "a", "b", "c", "d", "e"],
        )

    def test_decode_simple_string(self):
        self.assertEqual(decode(encode_simple("OK")), "OK")

    def test_decode_int(self):
        self.assertEqual(decode(encode(2)), 2)

    def test_decode_bulk_string(self):
        self.assertEqual(decode(encode("value")), "value")

    def test_decode_error(self):
        self.assertEqual(
            decode(encode(ValueError("error message"))).args,
            ValueError("error message").args,
        )

    def test_decode_command_batch(self):
        batch = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
        self.assertEqual(
            decode(batch),
            [["SET", "foo", "123"], ["SET", "bar", "456"], ["SET", "baz", "789"]],
            batch,
        )

    def test_decode_command_batch_with_rdb(self):
        batch = b"+FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0\r\n$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
        self.assertEqual(
            decode(batch),
            [
                "FULLRESYNC 75cd7bc10c49047e0d163660f3b90625b1af31dc 0",
                b"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2",
                ["REPLCONF", "GETACK", "*"],
            ],
            batch,
        )

    def test_decode_set(self):
        self.assertEqual(
            decode(b"*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n"),
            ["SET", "bar", "456"],
        ),

    def test_decode_commands_ping(self):
        data = b"*1\r\n$4\r\nPING\r\n"

        self.assertEqual(decode_commands(data), [(["PING"], 14)])

    def test_decode_commands_replconf(self):
        data = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

        self.assertEqual(decode_commands(data), [(["REPLCONF", "GETACK", "*"], 37)])

    def test_decode_commands_batch(self):
        batch = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n"
        self.assertEqual(
            decode_commands(batch),
            [
                (["SET", "foo", "123"], 31),
                (["SET", "bar", "456"], 31),
                (["SET", "baz", "789"], 31),
            ],
            batch,
        )


if __name__ == "__main__":
    unittest.main()
