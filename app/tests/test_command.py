import unittest

from app.command import BLPOP, ECHO, GET, LLEN, LPOP, LPUSH, LRANGE, PING, RPUSH, SET

from app.storage import storage


class TestCommand(unittest.IsolatedAsyncioTestCase):

    async def test_ping(self):
        self.assertEqual(await PING().execute(), b"+PONG\r\n")

    async def test_echo(self):
        self.assertEqual(await ECHO("hello", "world!").execute(), b"+hello world!\r\n")

    async def test_set_constructor(self):
        key, value = "foo", "bar"
        command = SET(key, value)
        self.assertEqual(command.key, key)
        self.assertEqual(command.value, value)
        self.assertEqual(command.ttlms, None)

    async def test_set_constructor_ttl_px(self):
        key, value = "foo", "bar"
        command = SET(key, value, "px", 2)
        self.assertEqual(command.key, key)
        self.assertEqual(command.value, value)
        self.assertEqual(command.ttlms, 2)

    async def test_set_constructor_ttl_ex(self):
        key, value = "foo", "bar"
        command = SET(key, value, "ex", 3)
        self.assertEqual(command.key, key)
        self.assertEqual(command.value, value)
        self.assertEqual(command.ttlms, 3_000)

    async def test_set(self):
        key, value = "foo", "bar"
        self.assertEqual(await SET(key, value).execute(), b"+OK\r\n")
        self.assertEqual(storage.get(key), value)

    async def test_set_zero_px_ttl(self):
        key, value = "foo", "bar"
        self.assertEqual(await SET(key, value, "px", "0").execute(), b"+OK\r\n")
        self.assertIsNone(storage.get(key))

    async def test_set_long_px_ttl(self):
        key, value = "foo", "bar"
        self.assertEqual(await SET(key, value, "px", "10").execute(), b"+OK\r\n")
        self.assertEqual(storage.get(key), value)

    async def test_get_exists(self):
        key, value = "foo", "bar"
        storage.set(key, value)
        self.assertEqual(await GET(key).execute(), b"$3\r\nbar\r\n")

    async def test_get_does_not_exist(self):
        key = "foo"
        self.assertEqual(await GET(key).execute(), b"$-1\r\n")

    async def test_llen_exists(self):
        key, values = "fruit", "apple banana strawberry".split()
        storage.set(key, values)
        self.assertEqual(await LLEN(key).execute(), f":{len(values)}\r\n".encode())

    async def test_llen_exists(self):
        key = "vegetables"
        self.assertEqual(await LLEN(key).execute(), b":0\r\n")

    async def test_lpop_exists(self):
        key, values = "fruit lpop", "apple banana strawberry".split()
        storage.set(key, values)
        self.assertEqual(await LPOP(key).execute(), b"$5\r\napple\r\n")

    async def test_lpop_does_not_exist(self):
        key = "fruit lpop does not exit"
        self.assertEqual(await LPOP(key).execute(), b"$-1\r\n")

    async def test_lpop_many_exists(self):
        key, values = "fruit lpop many", "apple banana strawberry".split()
        storage.set(key, values)
        self.assertEqual(
            await LPOP(key, "2").execute(), b"*2\r\n$5\r\napple\r\n$6\r\nbanana\r\n"
        )

    async def test_lrange_constractor(self):
        key, start, end = "my_list", "0", "-1"
        command = LRANGE(key, start, end)

        self.assertEqual(command.key, key)
        self.assertEqual(command.start, 0)
        self.assertEqual(command.end, -1)

    async def test_rpush_constractor(self):
        key, *items = ["my_list_rpush", "stone", "paper", "scissors"]
        command = RPUSH(key, *items)

        self.assertEqual(command.key, key)
        self.assertEqual(command.items, ["stone", "paper", "scissors"])

    async def test_lpush_constractor(self):
        key, *items = ["my_list_lpush", "Friede", "Freude", "Eierkuchen"]
        command = LPUSH(key, *items)

        self.assertEqual(command.key, key)
        self.assertEqual(command.items, ["Friede", "Freude", "Eierkuchen"])

    async def test_blpop_constractor_zero_timeout(self):
        key, timeout = "my_list_blpop", "0"
        command = BLPOP(key, timeout)

        self.assertEqual(command.key, key)
        self.assertEqual(command.timeout, None)

    async def test_blpop_constractor_non_zero_timeout(self):
        key, timeout = "my_list_blpop", "0.5"
        command = BLPOP(key, timeout)

        self.assertEqual(command.key, key)
        self.assertEqual(command.timeout, 0.5)


if __name__ == "__main__":
    unittest.main()
