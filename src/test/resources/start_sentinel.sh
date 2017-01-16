#!/bin/bash

for dir in node?; do 
	redis-server $dir/redis.conf > $(basename $dir).log &
	redis-server $dir/sentinel.conf --sentinel > $(basename $dir)_sentinel.log&
done
