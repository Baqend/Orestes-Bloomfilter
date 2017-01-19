For the non-sentinel tests you need to start a master and slave, copy the contents of the standalone directory to your Redis directory.

```sh
redis-server standalone/master.conf
redis-server standalone/conf.conf
```