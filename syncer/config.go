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
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("syncer", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.configFile, "config", "", "path to config file")
	fs.IntVar(&cfg.ServerID, "server-id", 101, "MySQL slave server ID")
	fs.IntVar(&cfg.WorkerCount, "c", 16, "parallel worker count")
	fs.IntVar(&cfg.Batch, "b", 10, "batch commit count")
	fs.StringVar(&cfg.StatusAddr, "status-addr", "", "status addr")
	fs.StringVar(&cfg.Meta, "meta", "syncer.meta", "syncer meta info")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	fs.StringVar(&cfg.FromDBType, "from-db-type", "mysql", "source database type, mysql or mariadb")

	return cfg
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

// TableName is the Table configuration
// slave restrict replication to a given table
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name"`
}

func (c DBConfig) String() string {
	return fmt.Sprintf("DBConfig(host:%s, user:%s, port:%d, pass:<omitted>)", c.Host, c.User, c.Port)
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ServerID int    `toml:"server-id" json:"server-id"`
	Meta     string `toml:"meta" json:"meta"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`
	Batch       int `toml:"batch" json:"batch"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	DoTables []TableName `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string    `toml:"replicate-do-db" json:"replicate-do-db"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-ignore-db
	IgnoreTables []TableName `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string    `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	SkipSQLs []string `toml:"skip-sqls" json:"skip-sqls"`

	From DBConfig `toml:"from" json:"from"`
	To   DBConfig `toml:"to" json:"to"`

	FromDBType string `toml:"from-db-type" json:"from-db-type"`

	configFile   string
	printVersion bool
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if c.printVersion {
		fmt.Printf(GetRawSyncerInfo())
		return flag.ErrHelp
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

	c.adjust()

	return nil
}

func (c *Config) adjust() {
	for i := 0; i < len(c.DoTables); i++ {
		c.DoTables[i].Name = strings.ToLower(c.DoTables[i].Name)
		c.DoTables[i].Schema = strings.ToLower(c.DoTables[i].Schema)
	}
	for i := 0; i < len(c.IgnoreTables); i++ {
		c.IgnoreTables[i].Name = strings.ToLower(c.IgnoreTables[i].Name)
		c.IgnoreTables[i].Schema = strings.ToLower(c.IgnoreTables[i].Schema)
	}
	for i := 0; i < len(c.IgnoreDBs); i++ {
		c.IgnoreDBs[i] = strings.ToLower(c.IgnoreDBs[i])
	}
	for i := 0; i < len(c.DoDBs); i++ {
		c.DoDBs[i] = strings.ToLower(c.DoDBs[i])
	}
}

func (c Config) String() string {
	return fmt.Sprintf(`log-level:%s log-file:%s log-rotate:%s status-addr:%s `+
		`server-id:%d worker-count:%d batch:%d meta-file:%s `+
		`do-tables:%v do-dbs:%v ignore-tables:%v ignore-dbs:%v `+
		`from:%s to:%s`,
		c.LogLevel, c.LogFile, c.LogRotate, c.StatusAddr,
		c.ServerID, c.WorkerCount, c.Batch, c.Meta,
		c.DoTables, c.DoDBs, c.IgnoreTables, c.IgnoreDBs,
		c.From, c.To)
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
