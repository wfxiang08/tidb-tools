## tidb-tools

tidb-tools are some useful tool collections for [TiDB](https://github.com/pingcap/tidb).


## How to build

```
make deps && sh deps.sh (optional, install golang dependent packages)
make build
```

## Tool list

[importer](./importer)

A tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

[syncer](./syncer)

A tool for syncing source database data to target database which is compatible with MySQL protocol, like MySQL, TiDB.

[checker](./checker)

A tool for checking the compatibility of an existed MySQL database with TiDB.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
