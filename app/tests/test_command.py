import asyncio
from os import path
import unittest

from app.args import Args
from app.command import (
    BLPOP,
    CONFIG,
    DISCARD,
    ECHO,
    EXEC,
    GET,
    INCR,
    INFO,
    KEYS,
    LLEN,
    LPOP,
    LPUSH,
    LRANGE,
    MULTI,
    PING,
    PSYNC,
    REPLCONF,
    RPUSH,
    SET,
    SUBSCRIBE,
    TYPE,
    WAIT,
    XADD,
    XRANGE,
    XREAD,
    CommandRegistry,
    Context,
    Session,
)

from app.resp import decode, encode, encode_simple
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

    async def test_registry_transaction(self):
        registry = CommandRegistry()
        registry.register(MULTI)
        registry.register(SET)
        registry.register(GET)
        registry.register(EXEC)

        transaction_id = 1
        context = Context(Args())
        session = Session()

        self.assertEqual(
            await registry.execute(transaction_id, context, session, "MULTI"),
            [encode_simple("OK")],
        )
        self.assertEqual(
            await registry.execute(
                transaction_id, context, session, "SET", "foo", "bar"
            ),
            [encode_simple("QUEUED")],
        )
        self.assertEqual(
            await registry.execute(transaction_id, context, session, "GET", "foo"),
            [encode_simple("QUEUED")],
        )
        self.assertEqual(
            await registry.execute(transaction_id, context, session, "EXEC"),
            [encode(["OK", "bar"])],
        )

        self.assertEqual(
            await registry.execute(transaction_id, context, session, "GET", "foo"),
            [encode("bar")],
        )

    async def test_registry_transaction_exec_without_multi(self):
        registry = CommandRegistry()
        registry.register(MULTI)
        registry.register(EXEC)

        transaction_id = 1

        self.assertEqual(
            await registry.execute(transaction_id, Context(Args()), Session(), "EXEC"),
            [encode(ValueError("EXEC without MULTI"))],
        )

    async def test_registry_transaction_discrad(self):
        registry = CommandRegistry()
        registry.register(MULTI)
        registry.register(SET)
        registry.register(GET)
        registry.register(DISCARD)

        transaction_id = 1
        context = Context(Args())
        session = Session()
        self.assertEqual(
            await registry.execute(transaction_id, context, session, "MULTI"),
            [encode_simple("OK")],
        )
        self.assertEqual(
            await registry.execute(
                transaction_id, context, session, "SET", "foo", "bar"
            ),
            [encode_simple("QUEUED")],
        )
        self.assertEqual(
            await registry.execute(transaction_id, context, session, "GET", "foo"),
            [encode_simple("QUEUED")],
        )
        self.assertEqual(
            await registry.execute(transaction_id, context, session, "DISCARD"),
            [encode_simple("OK")],
        )

    async def test_registry_transaction_discard_without_multi(self):
        registry = CommandRegistry()
        registry.register(MULTI)
        registry.register(DISCARD)

        transaction_id = 1
        context = Context(Args())
        session = Session()

        self.assertEqual(
            await registry.execute(transaction_id, context, session, "DISCARD"),
            [encode(ValueError("DISCARD without MULTI"))],
        )

    @unittest.expectedFailure
    def test_registry_register_duplicate(self):
        registry = CommandRegistry()
        registry.register(GET)

        registry.register(GET)

    async def test_ping(self):
        self.assertEqual(await PING().execute(), b"+PONG\r\n")

    async def test_ping_in_subscribed_mod(self):
        session = Session()
        session.subscribe("abc-channel")

        self.assertEqual(
            await PING().set_session(session).execute(),
            encode(["pong", ""]),
        )

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
        self.assertEqual(cmd.field_values, ("temperature", "36", "humidity", "95"))

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

        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$15\r\n1526919030474-0\r\n"
        )

        self.assertIsNotNone(storage.get(key))

    async def test_xadd_execute_sequence(self):
        key, idx, *field_values = "stream_key 1-1 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-1\r\n"
        )
        key, idx, *field_values = "stream_key 1-2 bar baz".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-2\r\n"
        )

        self.assertEqual(len(storage.get(key)), 2)

    async def test_xadd_execute_invalid_idx_zero_zero(self):
        key, idx, *field_values = "stream_key 0-0 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(),
            b"-ERR The ID specified in XADD must be greater than 0-0\r\n",
        )

    async def test_xadd_execute_invalid_idx_zero_zero_after_valid(self):
        key, idx, *field_values = "stream_key 1-0 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-0\r\n"
        )

        key, idx, *field_values = "stream_key 0-0 bar baz".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(),
            b"-ERR The ID specified in XADD must be greater than 0-0\r\n",
        )

    async def test_xadd_execute_invalid_duplicated_idx(self):
        key, idx, *field_values = "stream_key 1-1 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-1\r\n"
        )
        key, idx, *field_values = "stream_key 1-1 bar baz".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(),
            b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
        )

    async def test_xadd_execute_invalid_smaler_idx(self):
        key, idx, *field_values = "stream_key 1-1 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-1\r\n"
        )
        key, idx, *field_values = "stream_key 0-1 bar baz".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(),
            b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n",
        )

    async def test_xadd_execute_star_idx_seq_num_multiple(self):
        key, idx, *field_values = "stream_key 0-* foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n0-1\r\n"
        )

    async def test_xadd_execute_star_idx_seq_num_multiple(self):
        key, idx, *field_values = "stream_key 1-1 foo bar".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-1\r\n"
        )
        key, idx, *field_values = "stream_key 1-* bar baz".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n1-2\r\n"
        )
        key, idx, *field_values = "stream_key 2-* baz qux".split()
        self.assertEqual(
            await XADD(key, idx, *field_values).execute(), b"$3\r\n2-0\r\n"
        )

    async def test_xadd_execute_star(self):
        key, idx, *field_values = "stream_key * foo bar".split()
        idx_out = await XADD(key, idx, *field_values).execute()
        self.assertEqual(idx_out[:5], b"$15\r\n")

    async def test_xrange_constructor(self):
        key, start, end = "stream_key_xrange 0-2 0-3".split()
        cmd = XRANGE(key, start, end)

        self.assertEqual(cmd.key, key)
        self.assertEqual(cmd.start, start)
        self.assertEqual(cmd.end, end)

    async def test_xrange_constructor_minus(self):
        key, start, end = "stream_key_xrange - 0-3".split()
        cmd = XRANGE(key, start, end)

        self.assertEqual(cmd.key, key)
        self.assertEqual(cmd.start, "0-0")
        self.assertEqual(cmd.end, end)

    async def test_xrange_constructor_plus(self):
        key, start, end = "stream_key_xrange 0-1 +".split()
        cmd = XRANGE(key, start, end)

        self.assertEqual(cmd.key, key)
        self.assertEqual(cmd.start, start)
        self.assertEqual(cmd.end, "9" * 20)

    async def test_xrange(self):
        await XADD(*"stream_key_xrange 0-1 foo bar".split()).execute()
        await XADD(*"stream_key_xrange 0-2 bar baz".split()).execute()
        await XADD(*"stream_key_xrange 0-3 baz foo".split()).execute()

        encoded = await XRANGE(*"stream_key_xrange 0-2 0-3".split()).execute()

        self.assertEqual(
            encoded,
            b"*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n",
        )

    async def test_xrange(self):
        await XADD(*"stream_key_xrange 0-1 foo bar".split()).execute()
        await XADD(*"stream_key_xrange 0-2 bar baz".split()).execute()
        await XADD(*"stream_key_xrange 0-3 baz foo".split()).execute()

        encoded = await XRANGE(*"stream_key_xrange - +".split()).execute()

        self.assertEqual(
            encoded,
            b"*3\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n",
        )

    async def test_xrange_empty(self):
        encoded = await XRANGE(*"stream_key_xrange 0-2 0-3".split()).execute()
        self.assertEqual(encoded, b"*0\r\n")

    async def test_xread_constructor_streams(self):
        cmd = XREAD(*"streams stream_key other_stream_key 0-0 0-1".split())

        self.assertEqual(cmd.kind, "STREAMS")
        self.assertEqual(
            cmd.queries, (("stream_key", "0-0"), ("other_stream_key", "0-1"))
        )
        self.assertFalse(cmd.new_only)

    async def test_xread_constructor_block(self):
        cmd = XREAD(*"block 1000 streams stream_key other_stream_key 0-0 0-1".split())

        self.assertEqual(cmd.kind, "BLOCK")
        self.assertEqual(cmd.timeout, 1)
        self.assertEqual(
            cmd.queries, (("stream_key", "0-0"), ("other_stream_key", "0-1"))
        )
        self.assertFalse(cmd.new_only)

    async def test_xread_constructor_block_zero_timeout(self):
        cmd = XREAD(*"block 0 streams stream_key other_stream_key 0-0 0-1".split())

        self.assertEqual(cmd.kind, "BLOCK")
        self.assertIsNone(cmd.timeout)
        self.assertEqual(
            cmd.queries, (("stream_key", "0-0"), ("other_stream_key", "0-1"))
        )
        self.assertFalse(cmd.new_only)

    async def test_xread_constructor_block_new_only(self):
        cmd = XREAD(*"block 2500 streams stream_key other_stream_key 0-0 0-1 $".split())

        self.assertEqual(cmd.kind, "BLOCK")
        self.assertEqual(cmd.timeout, 2.5)
        self.assertEqual(
            cmd.queries, (("stream_key", "0-0"), ("other_stream_key", "0-1"))
        )
        self.assertTrue(cmd.new_only)

    async def test_xread(self):
        await XADD(*"stream_key_xrange 0-1 foo bar".split()).execute()
        await XADD(*"stream_key_xrange 0-2 bar baz".split()).execute()
        await XADD(*"stream_key_xrange 0-3 baz foo".split()).execute()

        encoded = await XREAD(*"streams stream_key_xrange 0-0".split()).execute()

        self.assertEqual(
            encoded,
            b"*1\r\n*2\r\n$17\r\nstream_key_xrange\r\n*3\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n",
        )

    async def test_xread_multiple(self):
        await XADD(*"stream_key_xrange_1 0-1 foo bar".split()).execute()
        await XADD(*"stream_key_xrange_1 0-2 bar baz".split()).execute()
        await XADD(*"stream_key_xrange_1 0-3 bar baz".split()).execute()
        await XADD(*"stream_key_xrange_2 0-3 baz foo".split()).execute()

        encoded = await XREAD(
            *"streams stream_key_xrange_1 stream_key_xrange_2 0-1 0-2".split()
        ).execute()

        self.assertEqual(
            encoded,
            b"*2\r\n*2\r\n$19\r\nstream_key_xrange_1\r\n*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$19\r\nstream_key_xrange_2\r\n*1\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n",
        )

    @unittest.skip
    async def test_xread_blocking(self):
        async with asyncio.TaskGroup() as task_group:
            xread = task_group.create_task(
                XREAD(*"block 1000 streams some_key 1526985054069-0".split()).execute()
            )
            _ = task_group.create_task(
                XADD(
                    *"some_key 1526985054079-0 temperature 37 humidity 94".split()
                ).execute()
            )

        self.assertEqual(
            xread.result(),
            b"*1\r\n*2\r\n$8\r\nsome_key\r\n*1\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n",
        )

    def test_incr_constructor(self):
        cmd = INCR("foo")

        self.assertEqual(cmd.key, "foo")

    async def test_incr_present(self):
        key = "foo-present"

        self.assertEqual(await SET(key, "1").execute(), b"+OK\r\n")

        self.assertEqual(await INCR(key).execute(), b":2\r\n")

        self.assertEqual(await GET(key).execute(), b"$1\r\n2\r\n")

    async def test_incr_missing(self):
        key = "foo-missing"

        self.assertEqual(await INCR(key).execute(), b":1\r\n")

        self.assertEqual(await GET(key).execute(), b"$1\r\n1\r\n")

    async def test_incr_non_int(self):
        key = "foo-non-int"

        self.assertEqual(await SET(key, "hello").execute(), b"+OK\r\n")

        self.assertEqual(
            await INCR(key).execute(),
            b"-ERR value is not an integer or out of range\r\n",
        )

        self.assertEqual(await GET(key).execute(), b"$5\r\nhello\r\n")

    async def test_multi(self):
        self.assertEqual(await MULTI().execute(), b"+OK\r\n")

    async def test_info_replication_master(self):
        args = Args()
        context = Context(args)
        self.assertEqual(
            await INFO("replication").set_context(context).execute(),
            encode(
                "role:master\n"
                "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n"
                "master_repl_offset:0"
            ),
        )

    async def test_info_replication_slave(self):
        args = Args()
        args.replicaof = "localhost 1234"
        context = Context(args)
        self.assertEqual(
            await INFO("replication").set_context(context).execute(),
            encode("role:slave"),
        )

    async def test_replconf_replica_ports(self):
        context = Context(Args())
        REPLCONF(*"listening-port 6767".split()).set_context(context)

    async def test_replconf(self):
        self.assertEqual(decode(await REPLCONF().execute()), "OK")

    async def test_replconf_getack(self):
        self.assertEqual(
            decode(await REPLCONF(*"GETACK 0".split()).execute()),
            "REPLCONF ACK 0".split(),
        )

    async def test_psync(self):
        payloads = await PSYNC().execute()

        self.assertEqual(
            decode(payloads[0]),
            "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
        )

        self.assertIsNotNone(payloads[1])

    async def test_wait_0_60000(self):
        cmd = WAIT(*"5 0".split())

        self.assertEqual(cmd.minreplicas, 5)
        self.assertIsNone(cmd.timeout)

    async def test_wait_0_60000(self):
        cmd = WAIT(*"0 60000".split())

        self.assertEqual(cmd.min_replicas, 0)
        self.assertEqual(cmd.timeout, 60)

        self.assertEqual(await cmd.execute(), encode(0))

    async def test_config(self):
        args = Args()
        args.dir = "/tmp"
        args.dbfilename = "dbfilename.rdb"
        context = Context(args)

        cmd = CONFIG(*"get dir dbfilename".split()).set_context(context)

        self.assertEqual(
            await cmd.execute(),
            encode(["dir", args.dir, "dbfilename", args.dbfilename]),
        )

    async def test_reading_keys_star_foo_bar(self):
        storage.load_from_rdb_dump("dumps", "foo-bar.rdb")
        self.assertEqual(await KEYS("*").execute(), encode(["foo"]))

    async def test_subscribe(self):
        session = Session()

        self.assertEqual(
            await SUBSCRIBE("mychan").set_session(session).execute(),
            encode(["subscribe", "mychan", 1]),
        )

    async def test_subscribe_multiple(self):
        session = Session()

        self.assertEqual(
            await SUBSCRIBE("my-chan1").set_session(session).execute(),
            encode(["subscribe", "my-chan1", 1]),
        )
        self.assertEqual(
            await SUBSCRIBE("my-chan2").set_session(session).execute(),
            encode(["subscribe", "my-chan2", 2]),
        )
        self.assertEqual(
            await SUBSCRIBE("my-chan1").set_session(session).execute(),
            encode(["subscribe", "my-chan1", 2]),
        )

    def test_session_subscribe(self):
        session = Session()

        self.assertEqual(session.subscribe("my_channel"), True)

        self.assertEqual(session.subscriptions(), 1)

    def test_session_subscribe_duplicate(self):
        session = Session()

        self.assertEqual(session.subscribe("my_channel_1"), True)
        self.assertEqual(session.subscribe("my_channel_2"), True)

        self.assertEqual(session.subscriptions(), 2)

        self.assertEqual(session.subscribe("my_channel_1"), False)
        self.assertEqual(session.subscriptions(), 2)


if __name__ == "__main__":
    unittest.main()
