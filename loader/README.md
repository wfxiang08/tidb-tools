## Loader
Loader is a tool used for multi-threaded restoration of mydumper.

## Options
Loader has the following available options:
```
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

## Config
```
# Loader log level
log-level = "info"

# Loader log file
log-file = ""

# Directory of the dump to import
dir = "./"

# Loader saved checkpoint
checkpoint = "loader.checkpoint"

# Loader pprof addr
pprof-addr = ":10084"

# Number of threads to use
worker = 4

# Number of queries per transcation
batch = 1000

# Skip unique index check
skip-unique-check = 0

# DB config
[db]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000
```

## Example
You can run the following commands:
```
./bin/loader -d ./test -h 127.0.0.1 -u root -P 4000
```
Or use the "config.toml" file:
```
./bin/loader -c=config.toml
```

## Notification
When loading the `mydumper` SQL file, Loader writes the file names which are successfully loaded into the checkpoint file. If Loader exits unexpectedly in the process of loading, we can recover it according to the checkpoint file and Loader will automatically ignore the files that have been loaded. The default checkpoint file is `loader.checkpoint`, you can change it by specifying the `-checkpoint` flag when you start Loader.

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
