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
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
)

var retryTimeout = 3 * time.Second

func executeSQL(db *sql.DB, sqls []string, retry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

	retryCount := 1
	if retry {
		retryCount = maxRetryCount
	}

LOOP:
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %v", i, sqls)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		for i := range sqls {
			log.Debugf("[exec][sql]%s", sqls[i])

			_, err = txn.Exec(sqls[i])
			if err != nil {
				log.Warnf("[exec][sql]%s[error]%v", sqls[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("[exec][sql]%s[error]%v", sqls[i], rerr)
				}
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("exec sqls[%v] failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return errors.Errorf("exec sqls[%v] failed", sqls)
}

func checkAndGetTableName(stmt *ast.InsertStmt) (string, bool) {
	ts, ok := stmt.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return "", false
	}

	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return "", false
	}

	return tn.Name.O, true
}

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}

func createDBs(cfg DBConfig, count int) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := createDB(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeDBs(dbs ...*sql.DB) {
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}
