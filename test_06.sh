#!/bin/sh

set -ex

redis-cli ping
redis-cli echo hello world!

redis-cli set foo bar
redis-cli set exp_key val px 100
redis-cli get exp_key
sleep 0.2
redis-cli get exp_key

redis-cli rpush my_list "1"
redis-cli rpush my_list "2"
redis-cli get my_list

redis-cli RPUSH list_key2 "a" "b" "c" "d" "e"
redis-cli LRANGE list_key2 0 1
redis-cli LRANGE list_key2 1 0
redis-cli LRANGE list_key2 10 1
redis-cli LRANGE list_key2 -2 -1
redis-cli LRANGE list_key2 0 -3
redis-cli LRANGE missing_list_key 0 1

redis-cli LPUSH list_key2 "m" "x"
redis-cli GET list_key2

redis-cli LLEN list_key2
redis-cli LPOP list_key2
redis-cli LLEN list_key2
redis-cli LPOP list_key2 2
redis-cli LLEN list_key2

redis-cli BLPOP list_key2 0
redis-cli BLPOP list_key3 0 &
redis-cli RPUSH list_key3 "foo"


redis-cli SET string_key hello
redis-cli TYPE string_key
redis-cli TYPE missing_key
redis-cli RPUSH list_key a b c
redis-cli Type list_key


redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
redis-cli Type stream_key

redis-cli XADD stream_key2 0-0 temperature 36 humidity 95
redis-cli XADD stream_key2 1-0 temperature 36 humidity 95
redis-cli XADD stream_key2 1-0 temperature 36 humidity 95
redis-cli XADD stream_key2 0-1 temperature 36 humidity 95
redis-cli XADD stream_key2 1-1 temperature 36 humidity 95
redis-cli XADD stream_key2 1-* temperature 36 humidity 95
redis-cli XADD pineapple 0-* pear banana

redis-cli XADD pineapple '*' pear banana


redis-cli XADD some_key 1526985054069-0 temperature 36 humidity 95
redis-cli XADD some_key 1526985054079-0 temperature 37 humidity 94
redis-cli XRANGE some_key 1526985054069 1526985054079

redis-cli XADD stream_key_xrange 0-1 foo bar
redis-cli XADD stream_key_xrange 0-2 bar baz
redis-cli XADD stream_key_xrange 0-3 baz foo
redis-cli XRANGE stream_key_xrange 0-2 0-3

redis-cli XRANGE stream_key_xrange - +

redis-cli XREAD streams stream_key_xrange 0-0
redis-cli XREAD streams stream_key_xrange stream_key_xrange 0-0 0-2

redis-cli set foo 1
redis-cli incr foo

redis-cli incr foo-missing

redis-cli multi
redis-cli set baz qux
redis-cli incr foo
redis-cli exec

redis-cli exec

redis-cli multi
redis-cli set baz qux
redis-cli incr foo
redis-cli discard

redis-cli discard

redis-cli info replication
