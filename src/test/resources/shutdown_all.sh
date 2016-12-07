#!/bin/sh

for port in 6380 6379  16385 16386 16387 6385 6386 6387; do
	nc -z 127.0.0.1 $port && echo shutdown $port && redis-cli -h 127.0.0.1 -p $port shutdown
done
