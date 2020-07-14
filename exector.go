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
	"context"
	"database/sql"
	"fmt"
	"strings"
	//"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// Import generates insert sqls and execute
func Import(db *sql.DB, tableSQLs []string, workerCount int, jobCount int64, batch int64, ratios map[string]float64, qps int64) {
	avg := false
	if len(ratios) == 0 {
		avg = true
	}

	jobChan := make(chan job, workerCount)
	jobDone := make(chan struct{}, len(tableSQLs))

	for i := range tableSQLs {
		go func(i int) {
			defer func() {
				jobDone <- struct{}{}
			}()

			table := newTable()
			err := parseTableSQL(table, tableSQLs[i])
			if err != nil {
				log.S().Errorf("parse sql %s failed, error %v", tableSQLs[i], errors.Trace(err))
				return
			}

			err = execSQL(db, "", tableSQLs[i])
			if err != nil {
				if strings.Contains(err.Error(), "already exists") {
					log.S().Warnf("table %s.%s already exists", table.schema, table.name)
				} else {
					log.S().Fatal(tableSQLs[i], err)
				}
			}

			ratio, ok := ratios[quoteSchemaTable(table.schema, table.name)] 
			if !ok {
				if avg {
					ratio = float64(1) / float64(len(tableSQLs))
					ok = true
				}
			}
			log.S().Infof("%s.%s ratio %f", table.schema, table.name, ratio)

			if ok {
				tableJobCount := int64(float64(jobCount) * ratio)
				if tableJobCount < 1 {
					tableJobCount = 1
				}
				generateJob(context.Background(), table, db, tableJobCount, batch, ratio, qps, jobChan)
			} else {
				generateJob(context.Background(), table, db, 1, batch, 1, 1, jobChan)
			}
		}(i)
	}

	doneNum := 0
	go func() {
		for {
			select {
			case <-jobDone:
				doneNum++
			default:
			}

			if doneNum >= len(tableSQLs) {
				close(jobChan)
				return
			}

			time.Sleep(time.Second)
		}
	}()

	doJobs(context.Background(), db, batch, workerCount, jobChan)
}

// TruncateTestTable truncates test data
func TruncateTestTable(db *sql.DB, schema string, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.S().Fatal(err)
		}

		err = execSQL(db, schema, fmt.Sprintf("truncate table %s.%s", table.schema, table.name))
		if err != nil {
			log.S().Fatal(err)
		}
	}
}

// DropTestTable drops test table
func DropTestTable(db *sql.DB, schema string, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.S().Fatal(err)
		}

		err = execSQL(db, schema, fmt.Sprintf("drop table %s", table.name))
		if err != nil {
			log.S().Fatal(err)
		}
	}
}
