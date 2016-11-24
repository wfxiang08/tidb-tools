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
)

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	log.Debugf("[query][sql]%s", query)

	rows, err = db.Query(query)
	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return rows, nil
}

func executeSQL(db *sql.DB, sqls []string, enableRetry bool, skipConstraintCheck bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var err error

	retryCount := 1
	if enableRetry {
		retryCount = maxRetryCount
	}

	if skipConstraintCheck {
		_, err = querySQL(db, "set @@session.tidb_skip_constraint_check=1;")
	} else {
		_, err = querySQL(db, "set @@session.tidb_skip_constraint_check=0;")
	}
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < retryCount; i++ {
		if i > 0 {

			log.Warnf("exec sql retry %d - %-.100v", i, sqls)
			time.Sleep(2 * time.Duration(i) * time.Second)
		}

		if err = executeSQLImp(db, sqls); err != nil {
			continue
		}

		return nil
	}
	if err != nil {
		log.Errorf("exec sqls[%-.100v] failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return errors.Errorf("exec sqls[%-.100v] failed", sqls)
}

func executeSQLImp(db *sql.DB, sqls []string) error {
	var (
		err error
		txn *sql.Tx
	)

	txn, err = db.Begin()
	if err != nil {
		log.Errorf("exec sqls[%-.100v] begin failed %v", sqls, errors.ErrorStack(err))
		return err
	}

	for i := range sqls {
		log.Debugf("[exec][sql]%-.100v", sqls)

		_, err = txn.Exec(sqls[i])
		if err != nil {
			log.Warnf("[exec][sql]%-.100v[error]%v", sqls, err)
			rerr := txn.Rollback()
			if rerr != nil {
				log.Errorf("[exec][sql]%-.100s[error]%v", sqls, rerr)
				return rerr
			}
			return err
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Errorf("exec sqls[%-.100v] commit failed %v", sqls, errors.ErrorStack(err))
		return err
	}

	return nil
}

func hasUniqIndex(db *sql.DB, schema string, table string) (bool, error) {
	if schema == "" || table == "" {
		return false, errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("show index from %s.%s", schema, table)
	rows, err := querySQL(db, query)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return false, errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/

	for rows.Next() {
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return false, errors.Trace(err)
		}

		nonUnique := string(datas[1])
		if nonUnique == "0" {
			return true, nil
		}
	}

	if rows.Err() != nil {
		return false, errors.Trace(rows.Err())
	}

	return false, nil
}

func truncateTable(db *sql.DB, schema string, table string) error {
	if schema == "" || table == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("truncate table `%s`.`%s`;", schema, table)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	log.Info(query)

	return nil
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
