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
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond
	maxWaitTime  = 3 * time.Second
	eventTimeout = 3 * time.Second
	statusTime   = 30 * time.Second

	maxDMLConnectionTimeout = "3s"
	maxDDLConnectionTimeout = "3h"
)

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.Mutex

	cfg *Config

	meta Meta

	syncer *replication.BinlogSyncer

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables map[string]*table

	fromDB *sql.DB
	toDBs  []*sql.DB
	ddlDB  *sql.DB

	done chan struct{}
	jobs []chan *job

	closed sync2.AtomicBool

	start    time.Time
	lastTime time.Time

	lastCount sync2.AtomicInt64
	count     sync2.AtomicInt64

	ctx    context.Context
	cancel context.CancelFunc

	patternMap map[string]*regexp.Regexp
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.meta = NewLocalMeta(cfg.Meta)
	syncer.closed.Set(false)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.done = make(chan struct{})

	// 定义不同的队列
	syncer.jobs = newJobChans(cfg.WorkerCount + 1)
	syncer.tables = make(map[string]*table)
	syncer.ctx, syncer.cancel = context.WithCancel(context.Background())
	syncer.patternMap = make(map[string]*regexp.Regexp)
	return syncer
}

func newJobChans(count int) []chan *job {
	jobs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		// 一次处理1000条数据更新
		jobs = append(jobs, make(chan *job, 1000))
	}

	return jobs
}

func closeJobChans(jobs []chan *job) {
	for _, ch := range jobs {
		close(ch)
	}
}

// Start starts syncer.
func (s *Syncer) Start() error {
	err := s.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	s.wg.Add(1)

	err = s.run()
	if err != nil {
		return errors.Trace(err)
	}

	// 类型: struct{}
	// 构造该类型的实例 struct{} {}
	s.done <- struct{}{}

	return nil
}

//
// 检查fromDB的binlog_format
//
func (s *Syncer) checkBinlogFormat() error {
	rows, err := s.fromDB.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return errors.Trace(err)
		}

		if variable == "binlog_format" && value != "ROW" {
			log.Fatalf("binlog_format is not 'ROW': %v", value)
		}

	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func (s *Syncer) clearTables() {
	s.tables = make(map[string]*table)
}

//
// 从数据库读取Table的信息
//
func (s *Syncer) getTableFromDB(db *sql.DB, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name

	err := s.getTableColumns(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = s.getTableIndex(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

// SHOW COLUMNS FROM xxx
//
func (s *Syncer) getTableColumns(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.schema, table.name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	idx := 0
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		column := &column{}
		column.idx = idx
		column.name = string(data[0])

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.unsigned = true
		}

		table.columns = append(table.columns, column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func (s *Syncer) getTableIndex(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.schema, table.name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
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
	var keyName string
	var columns []string
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			if keyName == "" {
				keyName = string(data[2])
			} else {
				if keyName != string(data[2]) {
					break
				}
			}

			columns = append(columns, string(data[4]))
		}
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.indexColumns = findColumns(table.columns, columns)
	return nil
}

func (s *Syncer) getTable(schema string, table string) (*table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, nil
	}

	db := s.toDBs[len(s.toDBs)-1]
	t, err := s.getTableFromDB(db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.tables[key] = t
	return t, nil
}

func (s *Syncer) addCount(tp opType, n int64) {
	switch tp {
	case insert:
		sqlJobsTotal.WithLabelValues("insert").Add(float64(n))
	case update:
		sqlJobsTotal.WithLabelValues("update").Add(float64(n))
	case del:
		sqlJobsTotal.WithLabelValues("del").Add(float64(n))
	case ddl:
		sqlJobsTotal.WithLabelValues("ddl").Add(float64(n))
	case xid, gtid:
	// skip xid, gtid jobs
	default:
		panic("unreachable")
	}

	s.count.Add(n)
}

func (s *Syncer) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.meta.Check() {
		return true
	}

	return false
}

func (s *Syncer) addJob(job *job) error {
	switch job.tp {
	// 这两个信息设计到重要的pos信息， 但是优先保存在内存中
	case xid:
		return s.meta.Save(job.pos, "", "", false)
	case gtid:
		return s.meta.Save(job.pos, job.gtid.id, job.gtid.gtid, false)

	case ddl:
		// while meet ddl, we should wait all dmls finished firstly
		s.jobWg.Wait()
	}

	if len(job.sql) > 0 {
		s.jobWg.Add(1)
		log.Debugf("add job [sql]%s; [position]%v", job.sql, job.pos)
		if job.tp == ddl {
			s.jobs[s.cfg.WorkerCount] <- job
		} else {
			idx := int(genHashKey(job.key)) % s.cfg.WorkerCount
			s.jobs[idx] <- job
		}
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		err := s.meta.Save(job.pos, "", "", true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (s *Syncer) sync(db *sql.DB, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()
	tpCnt := make(map[opType]int64)

	// sync函数中: 每一个job都对应jobWg中的一个计数器
	// executeSQL 可能一次执行多个job
	// 在执行完毕之后，clearF时可能要清除idx个jobWg的状态
	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			s.addCount(tpName, v)
			tpCnt[tpName] = 0
		}
	}

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			if job.tp == ddl {
				// 如果是ddl, 则将之前缓存的sql先执行完毕
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				// 单独执行ddl
				err = executeSQL(db, []string{job.sql}, [][]interface{}{job.args}, false)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}

				tpCnt[job.tp]++
				clearF()

			} else {
				// 继续buffer
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				tpCnt[job.tp]++

			}

			// 批量执行
			if idx >= count {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

			time.Sleep(waitTime)
		}
	}
}

func (s *Syncer) run() error {
	defer s.wg.Done()

	// 1. 创建replication sync
	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(s.cfg.ServerID),
		Flavor:   "mysql",
		Host:     s.cfg.From.Host,
		Port:     uint16(s.cfg.From.Port),
		User:     s.cfg.From.User,
		Password: s.cfg.From.Password,
	}

	s.syncer = replication.NewBinlogSyncer(&cfg)

	// 2. 创建数据库连接
	err := s.createDBs()
	if err != nil {
		return errors.Trace(err)
	}

	// 3. 检查binlog的格式
	err = s.checkBinlogFormat()
	if err != nil {
		return errors.Trace(err)
	}

	// support regex
	s.genRegexMap()

	// 5. 获取binlogStreamer
	streamer, isGTIDMode, err := s.getBinlogStreamer()
	if err != nil {
		return errors.Trace(err)
	}

	s.start = time.Now()
	s.lastTime = s.start

	s.wg.Add(s.cfg.WorkerCount)

	// 什么逻辑呢?
	// 不同的jobs的定义?
	for i := 0; i < s.cfg.WorkerCount; i++ {
		go s.sync(s.toDBs[i], s.jobs[i])
	}
	go s.sync(s.ddlDB, s.jobs[s.cfg.WorkerCount])

	s.wg.Add(1)
	go s.printStatus()

	pos := s.meta.Pos()
	// while meet GTIDEvent, save previous gtid to prevent losing binlog from syncer panicing
	var (
		id   string
		gtid string
	)
	var alreadyIgnoreAllRotateEvent bool
	for {
		// 主线程读取Event
		ctx, cancel := context.WithTimeout(s.ctx, eventTimeout)
		e, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.Canceled {
			log.Infof("ready to quit! [%v]", pos)
			return nil
		} else if err == context.DeadlineExceeded {
			continue
		}

		if err != nil {
			return errors.Trace(err)
		}

		// if gtid mode, should ignore the rotate events at head of event stream
		if !alreadyIgnoreAllRotateEvent && isGTIDMode {
			alreadyIgnoreAllRotateEvent = isNotRotateEvent(e)
			if !alreadyIgnoreAllRotateEvent {
				continue
			}
		}

		binlogMetaPos.Set(float64(e.Header.LogPos))

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			binlogEventsTotal.WithLabelValues("rotate").Inc()

			// 更新Position
			pos.Name = string(ev.NextLogName)
			pos.Pos = uint32(ev.Position)

			err = s.meta.Save(pos, "", "", true)
			if err != nil {
				return errors.Trace(err)
			}

			log.Infof("rotate binlog to %v", pos)
		case *replication.RowsEvent:
			// binlogEventsTotal.WithLabelValues("type", "rows").Add(1)

			//
			// binlog中的event带有的信息比较少，例如: @1='a', @2='b'
			// 如何和Table的Schema结合起来呢?
			//
			table := &table{}
			// 是否跳过呢?
			if s.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table)) {

				binlogSkippedEventsTotal.WithLabelValues("rows").Inc()
				if err = s.recordSkipSQLsPos(insert, pos); err != nil {
					return errors.Trace(err)
				}

				log.Warnf("[skip RowsEvent]db:%s table:%s", ev.Table.Schema, ev.Table.Table)
				continue
			}

			// 获取schema等信息呢?
			// schema如何响应ddl, 自动同步更新呢?
			//
			table, err = s.getTable(string(ev.Table.Schema), string(ev.Table.Table))

			if err != nil {
				return errors.Trace(err)
			}

			log.Debugf("schema: %s, table %s, RowsEvent data: %v", table.schema, table.name, ev.Rows)
			var (
				sqls []string
				keys []string
				args [][]interface{}
			)
			switch e.Header.EventType {
			// 处理各种格式的Write Rows, Update Rows, Delete Rows的操作
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:

				// 增加计数器
				binlogEventsTotal.WithLabelValues("write_rows").Inc()

				// 将binlog的数据转换成为明文的SQL
				sqls, keys, args, err = genInsertSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					job := newJob(insert, sqls[i], args[i], keys[i], true, pos)
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("update_rows").Inc()

				// 将binlog的数据转换成为明文的SQL
				sqls, keys, args, err = genUpdateSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					job := newJob(update, sqls[i], args[i], keys[i], true, pos)
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("delete_rows").Inc()

				// 将binlog的数据转换成为明文的SQL
				sqls, keys, args, err = genDeleteSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				// SQL语句到Job的分拆
				// TODO: 结合kingshard, 将可以实现一个同步Sharding的工具
				//   OriginDB <--> Shard0, Shard1, ..., ShardN-1
				//   通过binlog相互同步, 这样数据Sharding完毕之后，代码可以直接迁移；待部署完毕，可以停止OriginDB的更新
				// 如果只是定制，那么可以直接通过ev.Rows获取指定指定的信息来做Sharding, 不需要搞什么AST之类的东西
				for i := range sqls {
					job := newJob(del, sqls[i], args[i], keys[i], true, pos)
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			//
			// DDL为什么也归属于QueryEvent
			//
			binlogEventsTotal.WithLabelValues("query").Inc()

			// ok := false
			sql := string(ev.Query)

			log.Debugf("[query]%s", sql)

			lastPos := pos
			pos.Pos = e.Header.LogPos
			sqls, ok, err := resolveDDLSQL(sql)

			if err != nil {
				if s.skipQueryEvent(sql, string(ev.Schema)) {
					binlogSkippedEventsTotal.WithLabelValues("query").Inc()
					log.Warnf("[skip query-sql]%s  [schema]:%s", sql, string(ev.Schema))
					continue
				}
				return errors.Errorf("parse query event failed: %v, position %v", err, pos)
			}

			if !ok {
				continue
			}

			for _, sql := range sqls {
				if s.skipQueryDDL(sql, string(ev.Schema)) {
					binlogSkippedEventsTotal.WithLabelValues("query_ddl").Inc()
					if err = s.recordSkipSQLsPos(ddl, pos); err != nil {
						return errors.Trace(err)
					}

					log.Warnf("[skip query-ddl-sql]%s  [schema]:%s", sql, ev.Schema)
					continue
				}

				sql, err = genDDLSQL(sql, string(ev.Schema))
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[pos]%v[next pos]%v[schema]%s", sql, lastPos, pos, string(ev.Schema))

				job := newJob(ddl, sql, nil, "", false, pos)
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s[pos]%v[next pos]%v", sql, lastPos, pos)

				// DDL之后如何处理Tables的状态
				// 直接clear, 重来
				s.clearTables()
			}
		case *replication.XIDEvent:
			// XID: 这个似乎和重要
			pos.Pos = e.Header.LogPos
			job := newXIDJob(pos)
			s.addJob(job)
		case *replication.GTIDEvent:
			pos.Pos = e.Header.LogPos
			u, err := uuid.FromBytes(ev.SID)
			if err != nil {
				return errors.Trace(err)
			}
			// while meet GTIDEvent, save previous gtid to prevent losing binlog from syncer panicing
			job := newGTIDJob(id, gtid, pos)
			s.addJob(job)
			id = u.String()
			gtid = fmt.Sprintf("%s:1-%d", u.String(), ev.GNO)
			log.Debugf("gtid infomation: binlog %v, gtid %s", pos, gtid)
		}
	}
}

func (s *Syncer) genRegexMap() {
	for _, db := range s.cfg.DoDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := s.patternMap[db]; !ok {
			s.patternMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, db := range s.cfg.IgnoreDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := s.patternMap[db]; !ok {
			s.patternMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, tb := range s.cfg.DoTables {
		if tb.Name[0] == '~' {
			if _, ok := s.patternMap[tb.Name]; !ok {
				s.patternMap[tb.Name] = regexp.MustCompile(tb.Name[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := s.patternMap[tb.Schema]; !ok {
				s.patternMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}

	for _, tb := range s.cfg.IgnoreTables {
		if tb.Name[0] == '~' {
			if _, ok := s.patternMap[tb.Name]; !ok {
				s.patternMap[tb.Name] = regexp.MustCompile(tb.Name[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := s.patternMap[tb.Schema]; !ok {
				s.patternMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}
}

func (s *Syncer) printStatus() {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - s.lastTime.Unix()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			log.Infof("[syncer]total events = %d, total tps = %d, recent tps = %d, %s.",
				total, totalTps, tps, s.meta)

			s.lastCount.Set(total)
			s.lastTime = time.Now()
		}
	}
}

func (s *Syncer) getBinlogStreamer() (*replication.BinlogStreamer, bool, error) {
	gtidMap := s.meta.GTID()

	// 假设不使用GTID
	if s.cfg.EnableGTID && len(gtidMap) != 0 {
		var gtids []string
		for _, val := range gtidMap {
			gtids = append(gtids, val)
		}

		gtidSet, err := mysql.ParseMysqlGTIDSet(strings.Join(gtids, ","))
		if err != nil {
			log.Errorf("parse gtid %v error %v", gtids, err)
			return s.startSyncByPosition()
		}

		streamer, err := s.syncer.StartSyncGTID(gtidSet)
		if err != nil {
			log.Errorf("start sync in gtid mode error %v", err)
			return s.startSyncByPosition()
		}

		return streamer, true, err
	}

	return s.startSyncByPosition()
}

func (s *Syncer) createDBs() error {
	var err error
	// 创建fromDB
	s.fromDB, err = createDB(s.cfg.From, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	s.toDBs = make([]*sql.DB, 0, s.cfg.WorkerCount)

	// 根据workerCount来创建多个不同的sql.DB
	s.toDBs, err = createDBs(s.cfg.To, s.cfg.WorkerCount, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	// db for ddl
	s.ddlDB, err = createDB(s.cfg.To, maxDDLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *Syncer) recordSkipSQLsPos(op opType, pos mysql.Position) error {
	job := newJob(op, "", nil, "", false, pos)
	err := s.addJob(job)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Syncer) startSyncByPosition() (*replication.BinlogStreamer, bool, error) {
	// 开始同步
	streamer, err := s.syncer.StartSync(s.meta.Pos())
	return streamer, false, errors.Trace(err)
}

func (s *Syncer) isClosed() bool {
	return s.closed.Get()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed() {
		return
	}

	s.cancel()

	<-s.done

	closeJobChans(s.jobs)

	s.wg.Wait()

	closeDBs(s.fromDB)
	closeDBs(s.toDBs...)

	if s.syncer != nil {
		s.syncer.Close()
		s.syncer = nil
	}

	s.closed.Set(true)
}
