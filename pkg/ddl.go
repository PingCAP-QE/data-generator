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

	"github.com/pingcap/log"
)

func dml(ctx context.Context, db *sql.DB, id int) {
	var err error
	var i int
	var success int

	for i = 0; ; i++ {
		_, err = db.Exec("insert into test.test1(id) values(?)", i+id*100000000)
		if err == nil {
			success++
			if success%100 == 0 {
				log.S().Info(id, " success: ", success)
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
