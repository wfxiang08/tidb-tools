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
)

type dataJob struct {
	sql                 string
	schema              string
	skipConstraintCheck bool
}

type tableJob struct {
	schema     string
	table      string
	dataFiles  DataFiles
	startPos   int
	checkExist bool
}

var (
	jobCount      = 1000
	maxRetryCount = 10
)

// DataFiles represent all data files for a single table
type DataFiles []string

// Tables represent all data files of a table collection as a map
type Tables2DataFiles map[string]DataFiles

type WorkerPool struct {
	cfg        *Config
	checkPoint *CheckPoint
	conns      []*Conn
	wg         sync.WaitGroup
	JobQueue   chan *dataJob
}

func NewWorkerPool(cfg *Config, checkPoint *CheckPoint) (*WorkerPool, error) {
	pool := new(WorkerPool)
	pool.cfg = cfg
	pool.checkPoint = checkPoint
	conns, err := createConns(cfg.DB, cfg.PoolSize)
	if err != nil {
		return nil, err
	}
	pool.conns = conns
	pool.JobQueue = make(chan *dataJob, jobCount)

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
	go func() {
		for {
			job, ok := <-tableJobQueue
			if !ok {
				log.Infof("pool exit.")
				return
			}

			// restore a table
			checkExist := true
			for pos := job.startPos; pos < len(job.dataFiles); pos++ {
				if err := p.restoreDataFile(p.cfg.Dir, job.dataFiles[pos], job.schema, job.table, checkExist && job.checkExist); err != nil {
					log.Fatalf("restore data file (%v) failed, err: %v", job.dataFiles[pos], err)
				}
				checkExist = false
			}

			tableJobWg.Done()
		}
	}()
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

				j := &dataJob{
					sql:                 sql,
					schema:              schema,
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
	if err := l.prepare(); err != nil {
		log.Errorf("[loader] scan dir[%s] failed, err[%v]", l.cfg.Dir, err)
		return errors.Trace(err)
	}

	l.checkPoint.Calc(l.db2Tables)
	if err := l.initAndStartWorkerPools(); err != nil {
		log.Errorf("[loader] init and start worker pools failed, err[%v]", err)
		return errors.Trace(err)
	}

	if err := l.restoreData(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (l *Loader) initAndStartWorkerPools() error {
	for i := 0; i < l.cfg.PoolCount; i++ {
		pool, err := NewWorkerPool(l.cfg, l.checkPoint)
		if err != nil {
			return err
		}
		pool.run(l.tableJobQueue, l.tableJobWg)
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

func (l *Loader) restoreSchema(conn *Conn, sqlFile string, schema string) error {
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
				if len(schema) > 0 {
					sqls = append(sqls, fmt.Sprintf("use %s;", schema))
				}

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
		err = l.restoreSchema(conn, dbFile, "")
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
			key := strings.Join([]string{db, table}, ".")
			if _, ok := l.checkPoint.FinishedTables[key]; ok {
				log.Infof("table (%s) has finished, skip.", key)
				continue
			}

			startPos := 0
			checkExist := false
			sort.Strings(dataFiles)
			if pos, ok := l.checkPoint.PartialRestoredTables[key]; ok {
				hasUniqIdx, err := hasUniqIndex(conn, db, table)
				if err != nil {
					log.Fatalf("check unique index failed. err: %v", err)
				}
				if hasUniqIdx {
					startPos = pos
					checkExist = true
				} else {
					err = truncateTable(conn, db, table)
					if err != nil {
						log.Fatalf("trucate table (%s.%s) failed, err: %v", db, table, err)
					}
					startPos = 0
					checkExist = false
				}
			} else {
				// create table
				tableExist := false
				tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
				log.Infof("[loader][run table schema]%s[start]", tableFile)
				err := l.restoreSchema(conn, tableFile, db)
				if err != nil {
					if isErrTableExists(err) {
						log.Infof("[loader][table already exists, skip]%s", tableFile)
						tableExist = true
					} else {
						log.Fatalf("run table schema failed - %v", errors.ErrorStack(err))
					}
				}
				log.Infof("[loader][run table schema]%s[finished]", tableFile)

				if tableExist {
					hasUniqIdx, err := hasUniqIndex(conn, db, table)
					if err != nil {
						log.Fatalf("check unique index failed. err: %v", err)
					}
					if hasUniqIdx {
						startPos = 0
						checkExist = true
					} else {
						err = truncateTable(conn, db, table)
						if err != nil {
							log.Fatalf("trucate table (%s.%s) failed, err: %v", db, table, err)
						}
						startPos = 0
						checkExist = false
					}
				} else {
					startPos = 0
					checkExist = false
				}
			}

			j := &tableJob{
				schema:     db,
				table:      table,
				dataFiles:  dataFiles,
				startPos:   startPos,
				checkExist: checkExist,
			}
			l.dispatchTableJob(j)
		}
	}

	// wait all table restored.
	l.tableJobWg.Wait()

	log.Infof("All tables has restored.")

	return nil
}
