// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("loader", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.Dir, "d", "./", "Directory of the dump to import")

	fs.IntVar(&cfg.Batch, "q", 1000, "Number of queries per transaction")
	fs.IntVar(&cfg.Worker, "t", 4, "Number of threads to use")

	fs.StringVar(&cfg.DB.Host, "h", "127.0.0.1", "The host to connect to")
	fs.StringVar(&cfg.DB.User, "u", "root", "Username with privileges to run the dump")
	fs.StringVar(&cfg.DB.Password, "p", "", "User password")
	fs.IntVar(&cfg.DB.Port, "P", 4000, "TCP/IP port to connect to")

	fs.StringVar(&cfg.CheckPoint, "checkpoint", "loader.checkpoint", "store files that has restored")

	fs.StringVar(&cfg.PprofAddr, "pprof-addr", ":10084", "Loader pprof addr")
	fs.StringVar(&cfg.LogLevel, "L", "info", "Loader log level: debug, info, warn, error, fatal")

	return cfg
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host string `toml:"host" json:"host"`

	User string `toml:"user" json:"user"`

	Password string `toml:"password" json:"password"`

	Port int `toml:"port" json:"port"`
}

func (c *DBConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DBConfig(%+v)", *c)
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	LogFile string `toml:"log-file" json:"log-file"`

	PprofAddr string `toml:"pprof-addr" json:"pprof-addr"`

	Worker int `toml:"worker" json:"worker"`

	Batch int `toml:"batch" json:"batch"`

	Dir string `toml:"dir" json:"dir"`

	CheckPoint string `toml:"checkpoint" json:"checkpoint"`

	DB DBConfig `toml:"db" json:"db"`

	configFile string
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	// Load config file if specified.
	if c.configFile != "" {
		err = c.configFromFile(c.configFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
