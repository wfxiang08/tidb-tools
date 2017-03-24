## syncer

syncer is a tool for syncing source database data to target database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to use

```
Usage of syncer:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -V    prints version and exit
  -b int
        batch commit count (default 10)
  -c int
        parallel worker count (default 16)
  -config string
        path to config file
  -enable-gtid
        enable gtid mode
  -log-file string
        log file path
  -log-rotate string
        log file rotate type, hour/day (default "day")
  -meta string
        syncer meta info (default "syncer.meta")
  -server-id int
        MySQL slave server ID (default 101)
  -status-addr string
        status addr
```

## Config
```
log-level = "info"

server-id = 101

meta = "./syncer.meta"
worker-count = 16
batch = 10

pprof-addr = ":10081"

##replicate-do-db priority over replicate-do-table if have same db name
##and we support regex expression , start with '~' declare use regex expression.
#
#replicate-do-db = ["~^b.*","s1"]
#[[replicate-do-table]]
#db-name ="test"
#tbl-name = "log"

#[[replicate-do-table]]
#db-name ="test"
#tbl-name = "~^a.*"

# skip prefix mathched sqls
# skip-sqls = ["^ALTER\\s+USER", "^CREATE\\s+USER"]


[from]
host = "127.0.0.1"
user = "root"
password = ""
port = 3306

[to]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000
```

## Example

```
./bin/syncer -config=syncer/config.toml
```

## Notification

now syncer only supports ROW binlog format, doesn't supports GTID. You should make sure table has primary key or index, otherwise while syncer restart,it may inserts some duplicate data.

meta information save the position that had been synchronized. but it maybe not the latest position because syncer saves position information in memory firstly for performance.

syncer supports use regex expression to select database or table. the [Syntax reference](https://github.com/google/re2/wiki/Syntax) show some syntax are not supported.

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
