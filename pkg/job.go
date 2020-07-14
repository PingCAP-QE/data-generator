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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

/*
func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}
*/

func doSqls(table *table, db *sql.DB, batch, count int64) {
	sqlPrefix, datas, err := genInsertSqls(table, count)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
		return
	}

	t := time.Now()
	defer func() {
		log.S().Infof("%s.%s insert %d datas, cost %v", table.schema, table.name, len(datas), time.Since(t))
	}()

	for begin := 0; begin < len(datas); begin += int(batch) {
		end := begin + int(batch)
		if end > len(datas) {
			end = len(datas)
		}

		sql := fmt.Sprintf("%s %s;", sqlPrefix, strings.Join(datas[begin:end], ","))
		_, err = db.Exec(sql)
		if err == nil {
			continue
		}

		log.S().Errorf("%s.%s execute sql %s failed, %d rows is not inserted, error %v", table.schema, table.name, sqlPrefix, errors.ErrorStack(err))
		// tmp change
		//continue

		if !strings.Contains(err.Error(), "Duplicate entry") {
			continue
		}

		log.S().Warnf("%s.%s insert data have duplicate key, insert for every row", table.schema, table.name)
		for _, data := range datas[begin:end] {
			_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, data))
			if err != nil {
				log.S().Error(errors.ErrorStack(err))
			}
		}
	}
}

func execSqls(db *sql.DB, schema string, sqls []string, args [][]interface{}) {
	t := time.Now()
	defer func() {
		log.S().Infof("execute %d sqls, cost %v", len(sqls), time.Since(t))
	}()
	txn, err := db.Begin()
	if err != nil {
		log.S().Fatalf(errors.ErrorStack(err))
	}

	for i := range sqls {
		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.S().Errorf("sql: %s, args: %v, err: %v", sqls[i], args[i], errors.ErrorStack(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.S().Warn(errors.ErrorStack(err))
	}
}

func generateJob(ctx context.Context, table *table, db *sql.DB, jobCount int64, batch int64, ratio float64, qps int64, jobChan chan job) {

	if len(table.columns) <= 2 {
		log.S().Fatal("column count must > 2, and the first and second column are for primary key")
	}

	interval := int64(1)
	speed := int64(float64(qps) * ratio)
	if speed < 1 {
		interval = int64(1 / ratio)
		speed = speed * interval
	}

	var sc *SpeedControl
	if jobCount > 10 && interval < 10 {
		log.S().Infof("table %s.%s will insert %d rows every %d seconds", table.schema, table.name, speed, interval)
		sc = NewSpeedControl(speed, interval)
	}

	count := int64(0)

	t := time.Now()
	defer func() {
		log.S().Infof("generate %d rows for table %s.%s to insert, cost %v", count, table.schema, table.name, time.Since(t))
	}()

	for count < jobCount {
		num := int64(0)
		if sc != nil {
			num = sc.ApplyTokenSync()
		} else {
			num = jobCount
		}

		//log.S().Infof("table %s.%s apply %d", table.schema, table.name, num)

		if count+num > jobCount {
			num = jobCount - count
		}
		count += num

		sqlPrefix, datas, err := genInsertSqls(table, num)
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
			return
		}

		newJob := job{
			sqlPrefix: sqlPrefix,
			datas:     datas,
		}

		select {
		case <-ctx.Done():
			return
		case jobChan <- newJob:
		}
	}
}

type job struct {
	sqlPrefix string
	datas     []string
}

func doJobs(ctx context.Context, db *sql.DB, batch int64, workerCount int, allJobChan chan job) {
	t := time.Now()
	defer func() {
		log.S().Infof("all rows inserted, cost time %v", time.Since(t))
	}()

	var wg sync.WaitGroup
	jobChans := make([]chan job, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		jobChan := make(chan job, 20)
		jobChans = append(jobChans, jobChan)
		wg.Add(1)
		go func(threadNum int) {
			defer wg.Done()
			doJob(ctx, db, threadNum, batch, jobChan)
		}(i)
	}

	k := 0
	for {
		j, ok := <-allJobChan
		if !ok {
			for _, jobCh := range jobChans {
				close(jobCh)
			}
			break
		}

		jobs := splitJob(j, batch)
		for _, newJob := range jobs {
			jobChans[k%workerCount] <- newJob
			k++
		}
	}

	wg.Wait()
}

func splitJob(j job, batch int64) []job {
	jobs := make([]job, 0, 5)

	sqlPrefix := j.sqlPrefix
	datas := j.datas
	for begin := 0; begin < len(datas); begin += int(batch) {
		end := begin + int(batch)
		if end > len(datas) {
			end = len(datas)
		}

		jobs = append(jobs, job{
			sqlPrefix: sqlPrefix,
			datas:     datas[begin:end],
		})
	}

	return jobs
}

func doJob(ctx context.Context, db *sql.DB, id int, batch int64, jobChan chan job) {
	log.S().Infof("[start] thread %d is work", id)
	rowCount := 0
	txnCount := 0
	t := time.Now()
	executeDuration := time.Duration(0)

	defer func() {
		log.S().Infof("[Done] thread %d insert %d rows, %d transcations, total cost time %v, execute sql time %v", id, rowCount, txnCount, time.Since(t), executeDuration)
	}()

	ticker := time.NewTicker(time.Duration(10) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}

			beginT := time.Now()

			sqlPrefix := job.sqlPrefix
			datas := job.datas
			rowCount += len(datas)
			txnCount++

			sql := fmt.Sprintf("%s %s;", sqlPrefix, strings.Join(datas, ","))
			_, err := db.Exec(sql)
			if err == nil {
				executeDuration += time.Since(beginT)
				continue
			}

			log.S().Errorf("execute sql %s failed, %d rows is not inserted, error %v", sqlPrefix, len(datas), errors.ErrorStack(err))
			if !strings.Contains(err.Error(), "Duplicate entry") {
				executeDuration += time.Since(beginT)
				continue
			}

			log.S().Warnf("%s insert data have duplicate key, insert for every row", sqlPrefix)
			for _, data := range datas {
				_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, data))
				if err != nil {
					log.S().Error(errors.ErrorStack(err))
				}
			}
			executeDuration += time.Since(beginT)
		case <-ticker.C:
			log.S().Infof("[status] thread %d insert %d rows, %d transcations, cost time %v, execute sql time %v", id, rowCount, txnCount, time.Since(t), executeDuration)
		case <-ctx.Done():
			return
		}
	}
}
