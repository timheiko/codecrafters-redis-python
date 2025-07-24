import asyncio
import unittest

from app.command import (
    BLPOP,
    ECHO,
    GET,
    LLEN,
    LPOP,
    LPUSH,
    LRANGE,
    PING,
    RPUSH,
    SET,
    TYPE,
    XADD,
    CommandRegistry,
)

from app.storage import Stream, storage


class TestCommand(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        storage.clean()

    async def asyncTearDown(self):
        storage.clean()

    def test_registry_register(self):
        registry = CommandRegistry()
        registry.register(SET)
        self.assertIn("SET", registry)
        self.assertEqual(registry["SET"], SET)

    @unittest.expectedFailure
    def test_registry_register_duplicate(self):
        registry = CommandRegistry()
        registry.register(GET)

        registry.register(GET)

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

    async def test_blpop_non_blocking_rpush(self):
        key, value, timeout = "my_list_nonblocking_blpop", "apple", "0"
        storage.set(key, [value])

        self.assertEqual(
            await BLPOP(key, timeout).execute(),
            b"*2\r\n$25\r\nmy_list_nonblocking_blpop\r\n$5\r\napple\r\n",
        )

    async def test_blpop_blocking_rpush(self):
        key, value, timeout = "my_list_blpop_rpush", "mango", "1"

        async with asyncio.TaskGroup() as task_group:
            blpop = task_group.create_task(BLPOP(key, timeout).execute())
            task_group.create_task(RPUSH(key, value).execute())

        self.assertEqual(
            blpop.result(), b"*2\r\n$19\r\nmy_list_blpop_rpush\r\n$5\r\nmango\r\n"
        )

    async def test_blpop_blocking_lpush(self):
        key, value, timeout = "my_list_blpop_lpush", "pear", ".5"

        async with asyncio.TaskGroup() as task_group:
            blpop = task_group.create_task(BLPOP(key, timeout).execute())
            task_group.create_task(LPUSH(key, value).execute())

        self.assertEqual(
            blpop.result(), b"*2\r\n$19\r\nmy_list_blpop_lpush\r\n$4\r\npear\r\n"
        )

    async def test_type_missing(self):
        self.assertEqual(await TYPE("missing_key").execute(), b"+none\r\n")

    async def test_type_list(self):
        key, value = "orange", "ready"

        await RPUSH(key, value).execute()

        self.assertEqual(await TYPE(key).execute(), b"+list\r\n")

    async def test_type_string(self):
        key, value = "orange", "ready"

        await SET(key, value).execute()

        self.assertEqual(await TYPE(key).execute(), b"+string\r\n")

    async def test_type_stream(self):
        key = "temperature"

        storage.set(key, Stream())

        self.assertEqual(await TYPE(key).execute(), b"+stream\r\n")

    async def test_xadd_construtor_even_field_values(self):
        key, idx, *field_values = (
            "stream_key 1526919030474-0 temperature 36 humidity 95".split()
        )

        cmd = XADD(key, idx, *field_values)

        self.assertEqual(cmd.key, key)
        self.assertEqual(cmd.idx, idx)
        self.assertEqual(cmd.payload, (("temperature", "36"), ("humidity", "95")))

    @unittest.expectedFailure
    async def test_xadd_construtor_odd_field_values(self):
        key, idx, *field_values = (
            "stream_key 1526919030474-0 temperature 36 humidity 95 dangling-field".split()
        )

        XADD(key, idx, *field_values)

    async def test_xadd_execute(self):
        key, idx, *field_values = (
            "stream_key 1526919030474-0 temperature 36 humidity 95".split()
        )

        await XADD(key, idx, *field_values).execute()

        self.assertEqual(storage.get(key), Stream())


if __name__ == "__main__":
    unittest.main()
