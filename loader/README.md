## loader

## How to use

## Config
```
Usage of loader:
  -L string
    	Loader log level: debug, info, warn, error, fatal (default "info")
  -P int
    	TCP/IP port to connect to (default 4000)
  -d string
    	Directory of the dump to import (default "./")
  -h string
    	The host to connect to (default "127.0.0.1")
  -checkpoint string
    	Loader saved checkpoint (default "loader.checkpoint")
  -skip-unique-check
    	Skip unique index check (default 0)
  -p string
    	User password
  -pprof-addr string
    	Loader pprof addr (default ":10084")
  -q int
    	Number of queries per transaction (default 1000)
  -t int
    	Number of threads to use (default 4)
  -u string
    	Username with privileges to run the dump (default "root")
```

## Example
```
./bin/loader -d ./test -h 127.0.0.1 -u root -P 4000
```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
