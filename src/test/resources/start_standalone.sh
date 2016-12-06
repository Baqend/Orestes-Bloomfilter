#!/bin/sh
redis-server standalone/master.conf > master.log &
redis-server standalone/slave.conf > slave.log &
