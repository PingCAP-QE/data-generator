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
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jszwec/csvutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/chaos-mesh/data-generator/pkg"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	defaultStep = cfg.Step
	minInt64 = cfg.Base

	sourceDB, err := pkg.OpenDB(cfg.SourceDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := pkg.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()
	sourceDB.SetMaxOpenConns(100)
	sourceDB.SetMaxIdleConns(100)

	files, err := ioutil.ReadDir(cfg.TableSQLDir)
	if err != nil {
		log.S().Fatal(err)
	}

	tableSQLs := make([]string, 0, len(files))
	for _, f := range files {
		sql, err := analyzeSQLFile(filepath.Join(cfg.TableSQLDir, f.Name()))
		if err != nil {
			log.S().Fatal(err)
		}
		if len(sql) == 0 {
			log.S().Errorf("parse file %s get empty sql", f.Name())
			os.Exit(1)
		}
		tableSQLs = append(tableSQLs, sql)
	}

	tableRatio, err := analyzeRatioFile(cfg.RatioFile)
	if err != nil {
		log.S().Fatal(err)
	}

	Import(sourceDB, tableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch, tableRatio, cfg.QPS)

	log.S().Info("import finished!!!")
}

func analyzeSQLFile(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(realLine)...)
		if data[len(data)-1] == ';' {
			query := string(data)
			//data = data[0:0]
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				continue
			}
		}

	}

	return string(data), nil
}

func analyzeRatioFile(file string) (map[string]float64, error) {
	tablesMap := make(map[string]float64)
	if len(file) == 0 {
		return tablesMap, nil
	}
	type tableRatio struct {
		Schema string  `csv:"Db_name"`
		Table  string  `csv:"Table_name"`
		Ratio  float64 `csv:"Ratio"`
	}

	content, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	var tables []tableRatio
	if err := csvutil.Unmarshal(content, &tables); err != nil {
		return nil, err
	}

	for _, table := range tables {
		tablesMap[quoteSchemaTable(table.Schema, table.Table)] = table.Ratio
	}

	return tablesMap, nil
}

func quoteSchemaTable(schema, table string) string {
	if len(schema) == 0 {
		return ""
	}

	if len(table) > 0 {
		return fmt.Sprintf("`%s`.`%s`", schema, table)
	}

	return fmt.Sprintf("`%s`", schema)
}
