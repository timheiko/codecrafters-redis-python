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

redis-cli LPUSH list_key2 "m", "x"
redis-cli GET list_key2
