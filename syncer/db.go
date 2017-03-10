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
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

type opType byte

const (
	insert = iota + 1
	update
	del
	ddl
	xid
)

type job struct {
	tp    opType
	sql   string
	args  []interface{}
	key   string
	retry bool
	pos   gmysql.Position
	gtid  string
}

func newJob(tp opType, sql string, args []interface{}, key string, retry bool, pos gmysql.Position, gtid string) *job {
	return &job{tp: tp, sql: sql, args: args, key: key, retry: retry, pos: pos, gtid: gtid}
}

type column struct {
	idx      int
	name     string
	unsigned bool
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns []*column
}

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func columnValue(value interface{}, unsigned bool) string {
	castValue := castUnsigned(value, unsigned)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findColumn(columns []*column, indexColumn string) *column {
	for _, column := range columns {
		if column.name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*column, indexColumns []string) []*column {
	result := make([]*column, 0, len(indexColumns))

	for _, name := range indexColumns {
		column := findColumn(columns, name)
		if column != nil {
			result = append(result, column)
		}
	}

	return result
}

func genColumnList(columns []*column) string {
	var columnList []byte
	for i, column := range columns {
		name := fmt.Sprintf("`%s`", column.name)
		columnList = append(columnList, []byte(name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func genKeyList(columns []*column, dataSeq []interface{}) string {
	values := make([]string, 0, len(dataSeq))
	for i, data := range dataSeq {
		values = append(values, columnValue(data, columns[i].unsigned))
	}

	return strings.Join(values, ",")
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}

func genInsertSQLs(schema string, table string, dataSeq [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(dataSeq))
	keys := make([]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	for _, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("insert columns and data mismatch in length: %d vs %d", len(columns), len(data))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schema, table, columnList, columnPlaceholders)
		sqls = append(sqls, sql)
		values = append(values, value)

		keyColumns, keyValues := getColumnData(columns, indexColumns, value)
		keys = append(keys, genKeyList(keyColumns, keyValues))
	}

	return sqls, keys, values, nil
}

func getColumnData(columns []*column, indexColumns []*column, data []interface{}) ([]*column, []interface{}) {
	cols := make([]*column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, castUnsigned(data[column.idx], column.unsigned))
	}

	return cols, values
}

func genWhere(columns []*column, data []interface{}) string {
	var kvs bytes.Buffer
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "IS"
		}

		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` %s ?", columns[i].name, kvSplit)
		} else {
			fmt.Fprintf(&kvs, "`%s` %s ? AND ", columns[i].name, kvSplit)
		}
	}

	return kvs.String()
}

func genKVs(columns []*column) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i].name)
		} else {
			fmt.Fprintf(&kvs, "`%s` = ?, ", columns[i].name)
		}
	}

	return kvs.String()
}

func genUpdateSQLs(schema string, table string, data [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(data)/2)
	keys := make([]string, 0, len(data)/2)
	values := make([][]interface{}, 0, len(data)/2)
	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		newData := data[i+1]
		if len(oldData) != len(newData) {
			return nil, nil, nil, errors.Errorf("update data mismatch in length: %d vs %d", len(oldData), len(newData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, errors.Errorf("update columns and data mismatch in length: %d vs %d", len(columns), len(oldData))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		newValues := make([]interface{}, 0, len(newData))
		updateColumns := make([]*column, 0, len(indexColumns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, castUnsigned(oldData[j], columns[j].unsigned))
			newValues = append(newValues, castUnsigned(newData[j], columns[j].unsigned))
		}

		// ignore newData == oldData
		if len(updateColumns) == 0 {
			continue
		}

		value := make([]interface{}, 0, len(oldData))
		kvs := genKVs(updateColumns)
		value = append(value, newValues...)

		whereColumns, whereValues := updateColumns, oldValues
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnData(columns, indexColumns, oldData)
		}

		where := genWhere(whereColumns, whereValues)
		value = append(value, whereValues...)

		sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)

		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func genDeleteSQLs(schema string, table string, dataSeq [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(dataSeq))
	keys := make([]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	for _, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("delete columns and data mismatch in length: %d vs %d", len(columns), len(data))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		whereColumns, whereValues := columns, value
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnData(columns, indexColumns, value)
		}

		where := genWhere(whereColumns, whereValues)
		values = append(values, whereValues)

		sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;", schema, table, where)
		sqls = append(sqls, sql)
		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func ignoreDDLError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	default:
		return false
	}
}

func isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Errorf("[sql]%s[error]%v", sql, err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	return isDDL, nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
func resolveDDLSQL(sql string) (sqls []string, ok bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		log.Errorf("error while parsing sql: %s", sql)
		return nil, false, errors.Trace(err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	if !isDDL {
		sqls = append(sqls, sql)
		return
	}

	switch v := stmt.(type) {
	case *ast.DropTableStmt:
		var ex string
		if v.IfExists {
			ex = "IF EXISTS "
		}
		for _, t := range v.Tables {
			var db string
			if t.Schema.O != "" {
				db = fmt.Sprintf("`%s`.", t.Schema.O)
			}
			s := fmt.Sprintf("DROP TABLE %s%s`%s`", ex, db, t.Name.O)
			sqls = append(sqls, s)
		}
	case *ast.AlterTableStmt:
		tempSpecs := v.Specs
		if len(tempSpecs) <= 1 {
			sqls = append(sqls, sql)
			break
		}
		for i := range tempSpecs {
			v.Specs = tempSpecs[i : i+1]
			sql1 := alterTableStmtToSQL(v)
			log.Warnf("split alter table statement: %s", sql1)
			sqls = append(sqls, sql1)
		}
	default:
		sqls = append(sqls, sql)
	}
	return sqls, true, nil
}

func genDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}
	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}
	if schema == "" {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("USE `%s`; %s;", schema, sql), nil
}

func genTableName(schema string, table string) TableName {
	return TableName{Schema: schema, Name: table}

}

func parserDDLTableName(sql string) (TableName, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return TableName{}, errors.Trace(err)
	}

	var res TableName
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.DropDatabaseStmt:
		res = genTableName(v.Name, "")
	case *ast.CreateIndexStmt:
		res = genTableName(v.Table.Schema.L, v.Table.Name.L)
	case *ast.CreateTableStmt:
		res = genTableName(v.Table.Schema.L, v.Table.Name.L)
	case *ast.DropIndexStmt:
		res = genTableName(v.Table.Schema.L, v.Table.Name.L)
	case *ast.TruncateTableStmt:
		res = genTableName(v.Table.Schema.L, v.Table.Name.L)
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, errors.Errorf("drop table with multiple tables, may resovle ddl sql failed")
		}
		res = genTableName(v.Tables[0].Schema.L, v.Tables[0].Name.L)
	default:
		return res, errors.Errorf("unkown ddl type")
	}

	return res, nil
}

func isRetryableError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	}
	var e error
	for {
		e = errors.Cause(err)
		if err == e {
			break
		}
		err = e
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		if mysqlErr.Number == tmysql.ErrUnknown {
			return true
		}
		return false
	}

	return true
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("type", "query").Add(1)
			log.Warnf("sql query retry %d: %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Debugf("[query][sql]%s", query)

		rows, err = db.Query(query)
		if err != nil {
			if !isRetryableError(err) {
				return rows, errors.Trace(err)
			}
			log.Warnf("[query][sql]%s[error]%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

func executeSQL(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
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
			sqlRetriesTotal.WithLabelValues("stmt_exec").Add(1)
			log.Warnf("sql stmt_exec retry %d: %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		for i := range sqls {
			log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				if !isRetryableError(err) {
					rerr := txn.Rollback()
					if rerr != nil {
						log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
					}
					break LOOP
				}

				log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
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

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true", cfg.User, cfg.Password, cfg.Host, cfg.Port)
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
			log.Errorf("close db failed: %v", err)
		}
	}
}
