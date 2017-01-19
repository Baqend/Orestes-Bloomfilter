To startup the Sentinel for the RedisBFTest tests, copy the three directories node1, node2 and node3 to your Redis installation directory.
If you don't copy the files, Redis will add additional information on startup to the sentinel files that git would then want to checkin.

Then start up redis Sentinel by starting the master and the slaves:

```sh
redis-server node1/redis.conf
redis-server node1/sentinel.conf --sentinel

#Now the two slaves

redis-server node2/redis.conf
redis-server node2/sentinel.conf --sentinel

redis-server node3/redis.conf
redis-server node3/sentinel.conf --sentinel
```