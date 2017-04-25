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
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/tableroute"
)

var (
	jobCount      = 1000
	maxRetryCount = 10
)

// Set represents a set in mathematics.
type Set map[string]struct{}

// DataFiles represent all data files for a single table
type DataFiles []string

// Tables2DataFiles represent all data files of a table collection as a map
type Tables2DataFiles map[string]DataFiles

type dataJob struct {
	sql                 string
	schema              string
	skipConstraintCheck bool
}

type tableJob struct {
	schema        string
	table         string
	dataFiles     DataFiles
	restoredFiles Set
	startPos      int
	endPos        int
	checkExist    bool
}

// WorkerPool represents a worker pool.
type WorkerPool struct {
	cfg        *Config
	checkPoint *CheckPoint
	conns      []*Conn
	wg         sync.WaitGroup
	JobQueue   chan *dataJob

	tableRouter route.TableRouter
}

func NewWorkerPool(cfg *Config, checkPoint *CheckPoint, tableRouter route.TableRouter) (*WorkerPool, error) {
	pool := new(WorkerPool)
	pool.cfg = cfg
	pool.checkPoint = checkPoint
	conns, err := createConns(cfg.DB, cfg.PoolSize)
	if err != nil {
		return nil, err
	}
	pool.conns = conns
	pool.JobQueue = make(chan *dataJob, jobCount)

	pool.tableRouter = tableRouter

	return pool, nil
}

func (p *WorkerPool) run(tableJobQueue chan *tableJob, tableJobWg *sync.WaitGroup) {
	// pool worker routines
	for i := 0; i < p.cfg.PoolSize; i++ {
		go func(workerId int) {
			for {
				job, ok := <-p.JobQueue
				if !ok {
					log.Infof("worker exit")
					return
				}
				sqls := make([]string, 0, 2)
				sqls = append(sqls, fmt.Sprintf("USE %s;", job.schema))
				sqls = append(sqls, job.sql)
				if err := executeSQL(p.conns[workerId], sqls, true, job.skipConstraintCheck); err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				p.wg.Done()
			}
		}(i)
	}

	// pool main routine
	for {
		job, ok := <-tableJobQueue
		if !ok {
			log.Infof("pool exit.")
			return
		}

		// restore a table
		checkExist := true
		for pos := job.startPos; pos < job.endPos; pos++ {
			if _, ok := job.restoredFiles[job.dataFiles[pos]]; ok {
				continue
			}
			if err := p.restoreDataFile(p.cfg.Dir, job.dataFiles[pos], job.schema, job.table, checkExist && job.checkExist); err != nil {
				log.Fatalf("restore data file (%v) failed, err: %v", job.dataFiles[pos], err)
			}
			checkExist = false
		}

		tableJobWg.Done()
	}
}

func (p *WorkerPool) restoreDataFile(path, dataFile, schema, table string, checkExist bool) error {
	log.Infof("[loader][restore table data sql]%s/%s[start]", path, dataFile)

	err := p.dispatchSQL(fmt.Sprintf("%s/%s", p.cfg.Dir, dataFile), schema, table, checkExist)
	if err != nil {
		return errors.Trace(err)
	}

	p.wg.Wait()

	err = p.checkPoint.Save(dataFile)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[loader][restore table data sql]%s/%s[finished]", p.cfg.Dir, dataFile)
	return nil
}

func (p *WorkerPool) dispatchSQL(file, schema, table string, checkExist bool) error {
	f, err := os.Open(file)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.TrimSpace(line[:len(line)-1])
			if len(realLine) == 0 {
				continue
			}

			data = append(data, []byte(realLine)...)
			if data[len(data)-1] == ';' {
				query := string(data)
				data = data[0:0]
				if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
					continue
				}

				idx := strings.Index(query, "INSERT INTO")
				if idx < 0 {
					return errors.Errorf("[invalid insert sql][sql]%s", query)
				}

				var sql string
				if checkExist {
					sql = fmt.Sprintf("INSERT IGNORE INTO %s", query[idx+len("INSERT INTO"):])
				} else {
					sql = query
				}

				sql = renameShardingTable(p.tableRouter, sql, schema, table)
				log.Debugf("sql: %-.100v", sql)

				targetSchema, _ := fetchMatchedLiteral(p.tableRouter, schema, table)

				j := &dataJob{
					sql:                 sql,
					schema:              targetSchema,
					skipConstraintCheck: p.cfg.SkipConstraintCheck == 1,
				}
				if checkExist {
					j.skipConstraintCheck = false
				}
				p.dispatchJob(j)
			}
		}
	}

	return nil
}

func (p *WorkerPool) dispatchJob(job *dataJob) {
	p.wg.Add(1)
	p.JobQueue <- job
}

// Loader can load your mydumper data into MySQL database.
type Loader struct {
	sync.Mutex

	cfg        *Config
	checkPoint *CheckPoint

	// db -> tables
	// table -> data files
	db2Tables map[string]Tables2DataFiles

	tableJobWg    *sync.WaitGroup
	tableJobQueue chan *tableJob

	tableRouter route.TableRouter

	pools []*WorkerPool
}

// NewLoader creates a new Loader.
func NewLoader(cfg *Config) *Loader {
	loader := new(Loader)
	loader.cfg = cfg
	loader.checkPoint = newCheckPoint(cfg.CheckPoint)

	loader.db2Tables = make(map[string]Tables2DataFiles)
	loader.tableJobQueue = make(chan *tableJob, jobCount)
	loader.tableJobWg = new(sync.WaitGroup)
	loader.pools = make([]*WorkerPool, 0, cfg.PoolCount)

	return loader
}

// Restore begins the restore process.
func (l *Loader) Restore() error {
	err := l.genRouter(l.cfg.RouteRules)
	if err != nil {
		return errors.Trace(err)
	}

	if err := l.prepare(); err != nil {
		log.Errorf("[loader] scan dir[%s] failed, err[%v]", l.cfg.Dir, err)
		return errors.Trace(err)
	}

	// check last file-num-per-block and current file-num-per-block, they must equal
	if l.checkPoint.FileNumPerBlock <= 0 {
		l.checkPoint.SaveFileNumPerBlock(l.cfg.FileNumPerBlock)
	} else if l.checkPoint.FileNumPerBlock != l.cfg.FileNumPerBlock {
		log.Fatalf(`[loader] last FileNumPerBlock is (%d), but current FileNumPerBlock is (%d),
		please restart loader with FileNumPerBlock equal to the last time.`,
			l.checkPoint.FileNumPerBlock, l.cfg.FileNumPerBlock)
	}

	l.checkPoint.CalcProgress(l.db2Tables)
	if err := l.initAndStartWorkerPools(); err != nil {
		log.Errorf("[loader] init and start worker pools failed, err[%v]", err)
		return errors.Trace(err)
	}

	if err := l.restoreData(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (l *Loader) genRouter(rules []*RouteRule) error {
	l.tableRouter = route.NewTrieRouter()

	for _, rule := range rules {
		err := l.tableRouter.Insert(rule.PatternSchema, rule.PatternTable, rule.TargetSchema, rule.TargetTable)
		if err != nil {
			return errors.Trace(err)
		}
	}
	log.Debugf("table_router rules:%+v", l.tableRouter.AllRules())
	return nil
}

func (l *Loader) initAndStartWorkerPools() error {
	for i := 0; i < l.cfg.PoolCount; i++ {
		pool, err := NewWorkerPool(l.cfg, l.checkPoint, l.tableRouter)
		if err != nil {
			return err
		}

		go pool.run(l.tableJobQueue, l.tableJobWg)

		l.pools = append(l.pools, pool)
	}
	return nil
}

func (l *Loader) prepareDbFiles(files map[string]struct{}) error {
	for file := range files {
		if !strings.HasSuffix(file, "-schema-create.sql") {
			continue
		}

		idx := strings.Index(file, "-schema-create.sql")
		if idx > 0 {
			db := file[:idx]
			l.db2Tables[db] = make(Tables2DataFiles)
		}
	}

	if len(l.db2Tables) == 0 {
		return errors.New("invalid mydumper files")
	}

	return nil
}

func (l *Loader) prepareTableFiles(files map[string]struct{}) error {
	for file := range files {
		if !strings.HasSuffix(file, "-schema.sql") {
			continue
		}

		idx := strings.Index(file, "-schema.sql")
		name := file[:idx]
		fields := strings.Split(name, ".")
		if len(fields) != 2 {
			log.Warnf("invalid table schema file - %s", file)
			continue
		}

		db, table := fields[0], fields[1]
		tables, ok := l.db2Tables[db]
		if !ok {
			return errors.Errorf("invalid table schema file, cannot find db - %s", file)
		}

		if _, ok := tables[table]; ok {
			return errors.Errorf("invalid table schema file, duplicated item - %s", file)
		}
		tables[table] = make(DataFiles, 0, 16)
	}

	return nil
}

func (l *Loader) prepareDataFiles(files map[string]struct{}) error {
	for file := range files {
		if !strings.HasSuffix(file, ".sql") || strings.Index(file, "-schema.sql") >= 0 ||
			strings.Index(file, "-schema-create.sql") >= 0 {
			continue
		}

		// ignore view / triggers
		if strings.Index(file, "-schema-view.sql") >= 0 || strings.Index(file, "-schema-triggers.sql") >= 0 ||
			strings.Index(file, "-schema-post.sql") >= 0 {
			log.Warnf("[loader] ignore unsupport view/trigger: %s", file)
			continue
		}

		idx := strings.Index(file, ".sql")
		name := file[:idx]
		fields := strings.Split(name, ".")
		if len(fields) != 2 && len(fields) != 3 {
			log.Warnf("invalid db table sql file - %s", file)
			continue
		}

		db, table := fields[0], fields[1]
		tables, ok := l.db2Tables[db]
		if !ok {
			return errors.Errorf("invalid data sql file, cannot find db - %s", file)
		}

		dataFiles, ok := tables[table]
		if !ok {
			return errors.Errorf("invalid data sql file, cannot find table - %s", file)
		}
		dataFiles = append(dataFiles, file)
		tables[table] = dataFiles
	}

	return nil
}

func (l *Loader) prepare() error {
	// check if mydumper dir data exists.
	if !IsDirExists(l.cfg.Dir) {
		return errors.New("empty mydumper dir")
	}

	// collect dir files.
	files := CollectDirFiles(l.cfg.Dir)
	_, ok := files["metadata"]
	if !ok {
		return errors.New("invalid mydumper dir, none metadata exists")
	}

	log.Debugf("collected files:%+v", files)

	/* Mydumper file names format
	 * db    {db}-schema-create.sql
	 * table {db}.{table}-schema.sql
	 * sql   {db}.{table}.{part}.sql or {db}.{table}.sql
	 */

	// Sql file for create db
	if err := l.prepareDbFiles(files); err != nil {
		return err
	}

	// Sql file for create table
	if err := l.prepareTableFiles(files); err != nil {
		return err
	}

	// Sql file for restore data
	return l.prepareDataFiles(files)
}

func (l *Loader) restoreSchema(conn *Conn, sqlFile string, schema string, table string) error {
	f, err := os.Open(sqlFile)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			realLine := strings.TrimSpace(line[:len(line)-1])
			if len(realLine) == 0 {
				continue
			}

			data = append(data, []byte(realLine)...)
			if data[len(data)-1] == ';' {
				query := string(data)
				data = data[0:0]
				if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
					continue
				}

				var sqls []string

				if table != "" {
					// for table
					query = renameShardingTable(l.tableRouter, query, schema, table)
					schema, _ = fetchMatchedLiteral(l.tableRouter, schema, table)
					sqls = append(sqls, fmt.Sprintf("use %s;", schema))
				} else {
					// for schema
					query = renameShardingSchema(l.tableRouter, query, schema, table)
				}

				log.Debugf("query:%s", query)

				sqls = append(sqls, query)
				err = executeSQL(conn, sqls, false, false)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	return nil
}

// renameShardingTable renames table name like `table-001`` to `table` by modifying query.
func renameShardingTable(router route.TableRouter, query, schema, table string) string {
	targetSchema, targetTable := fetchMatchedLiteral(router, schema, table)
	log.Debugf("table router match. origin_table:%s,targetTable:%s, origin_schema:%s, targetSchema:%s,query:%-.100v",
		table, targetTable, schema, targetSchema, query)

	return SQLReplace(query, table, targetTable)
}

func renameShardingSchema(router route.TableRouter, query, schema, table string) string {
	targetSchema, targetTable := fetchMatchedLiteral(router, schema, table)
	log.Debugf("table router match. origin_table:%s,targetTable:%s, origin_schema:%s, targetSchema:%s,query:%-.100v",
		table, targetTable, schema, targetSchema, query)

	return SQLReplace(query, schema, targetSchema)
}

func fetchMatchedLiteral(router route.TableRouter, schema, table string) (string, string) {
	schema, table = toLower(schema, table)
	if schema == "" {
		// nothing change
		return schema, table
	}
	targetSchema, targetTable := router.Match(schema, table)
	if targetSchema == "" {
		// nothing change
		return schema, table
	}
	if targetTable == "" {
		// table still same;
		targetTable = table
	}

	return targetSchema, targetTable
}

func toLower(schema, table string) (string, string) {
	return strings.ToLower(schema), strings.ToLower(table)
}

func causeErr(err error) error {
	var e error
	for {
		e = errors.Cause(err)
		if err == e {
			break
		} else {
			err = e
		}
	}
	return e
}

func (l *Loader) dispatchTableJob(job *tableJob) {
	l.tableJobWg.Add(1)
	l.tableJobQueue <- job
}

func (l *Loader) restoreData() error {
	var err error
	conn, err := createConn(l.cfg.DB)
	if err != nil {
		return errors.Trace(err)
	}

	// restore db in sort
	dbs := make([]string, 0, len(l.db2Tables))
	for db := range l.db2Tables {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		tables := l.db2Tables[db]

		// create db
		dbFile := fmt.Sprintf("%s/%s-schema-create.sql", l.cfg.Dir, db)
		log.Infof("[loader][run db schema]%s[start]", dbFile)
		err = l.restoreSchema(conn, dbFile, db, "")
		if err != nil {
			if isErrDBExists(err) {
				log.Infof("[loader][database already exists, skip]%s", dbFile)
			} else {
				log.Fatalf("run db schema failed - %v", errors.ErrorStack(err))
			}
		}
		log.Infof("[loader][run db schema]%s[finished]", dbFile)

		// restore table in sort
		tnames := make([]string, 0, len(tables))
		for t := range tables {
			tnames = append(tnames, t)
		}
		sort.Strings(tnames)
		for _, table := range tnames {
			dataFiles := tables[table]

			if l.checkPoint.IsTableFinished(db, table) {
				log.Infof("table (%s.%s) has finished, skip.", db, table)
				continue
			}

			// create table
			tableExist := false
			tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
			err := l.restoreSchema(conn, tableFile, db, table)
			if err != nil {
				if isErrTableExists(err) {
					log.Infof("[loader][table already exists, skip]%s", tableFile)
					tableExist = true
				} else {
					log.Fatalf("run table schema failed - %v", errors.ErrorStack(err))
				}
			}
			log.Infof("[loader][run table schema]%s[finished]", tableFile)

			// check if has unique index
			hasUniqIdx, err := hasUniqIndex(conn, db, table, l.tableRouter)
			if err != nil {
				log.Fatalf("check unique index failed. err: %v", err)
			}

			restoredFiles := l.checkPoint.GetRestoredFiles(db, table)

			// if partial data has restored for table that not have unique index, we must truncate the table
			// and restart from the very begin.
			if tableExist && !hasUniqIdx {
				err = truncateTable(conn, db, table)
				if err != nil {
					log.Fatalf("truncate table (%s.%s) failed, err: %v", db, table, err)
				}

				restoredFiles = make(Set)
			}

			// split this table into multi blocks and restore concurrently
			sort.Strings(dataFiles)
			for startPos, endPos := 0, 0; startPos < len(dataFiles); startPos = endPos {
				endPos = startPos + l.cfg.FileNumPerBlock
				if endPos > len(dataFiles) {
					endPos = len(dataFiles)
				}

				log.Debugf("dispatch table job. schema:%s, table:%s, files:%+v", db, table, dataFiles)
				log.Debugf("restored files:%+v", restoredFiles)

				j := &tableJob{
					schema:        db,
					table:         table,
					dataFiles:     dataFiles,
					restoredFiles: restoredFiles,
					startPos:      startPos,
					endPos:        endPos,
					checkExist:    tableExist && hasUniqIdx,
				}
				l.dispatchTableJob(j)
			}
		}
	}

	// wait all table restored.
	l.tableJobWg.Wait()

	log.Infof("All tables has restored.")

	return nil
}
