// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/errors"

	"github.com/chaos-mesh/data-generator/pkg"

)

// NewConfig creates a new config.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("binlogTest", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.configFile, "config", "", "Config file")
	fs.IntVar(&cfg.WorkerCount, "c", 1, "parallel worker count")
	fs.Int64Var(&cfg.JobCount, "n", 1, "total job count")
	fs.Int64Var(&cfg.Batch, "b", 1, "insert batch commit count")
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.Int64Var(&cfg.Step, "step", 1, "step")
	fs.Int64Var(&cfg.Base, "base", 1, "base")

	return cfg
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel string `toml:"log-level" json:"log-level"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`

	JobCount int64 `toml:"job-count" json:"job-count"`

	Batch int64 `toml:"batch" json:"batch"`

	SourceDBCfg pkg.DBConfig `toml:"source-db" json:"source-db"`

	//TargetDBCfg DBConfig `toml:"target-db" json:"target-db"`

	TableSQLDir string `toml:"table-sql-dir" json:"table-sql-dir"`

	RatioFile string `toml:"ratio-file" json:"ratio-file"`

	QPS int64 `toml:"qps" json:"qps"`

	Step int64 `toml:"step" json:"step"`

	Base int64 `toml:"base" json:"base"`

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
