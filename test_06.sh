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