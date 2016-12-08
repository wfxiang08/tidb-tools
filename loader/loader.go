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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go/sync2"
)

type job struct {
	sql                 string
	schema              string
	skipConstraintCheck bool
}

var (
	jobCount      = 1000
	maxRetryCount = 10

	waitTime    = 50 * time.Millisecond
	maxWaitTime = 1 * time.Second
)

type DataFiles []string
type Tables map[string]DataFiles

// Loader can load your mydumper data into MySQL database.
type Loader struct {
	sync.Mutex

	cfg *Config

	checkPoint *CheckPoint

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	// db -> tables
	// table -> data files
	dbTables map[string]Tables

	restoredFiles map[string]struct{}

	conns []*Conn

	jobsQueue chan *job

	closed sync2.AtomicBool

	firstDataFile         bool
	restoreFromCheckpoint bool
}

// NewLoader creates a new Loader.
func NewLoader(cfg *Config) *Loader {
	loader := new(Loader)
	loader.cfg = cfg
	loader.closed.Set(false)
	loader.checkPoint = newCheckPoint(cfg.CheckPoint)
	loader.restoreFromCheckpoint = loader.checkPoint.IsRestoreFromLastCheckPoint()
	loader.dbTables = make(map[string]Tables)
	loader.restoredFiles = loader.checkPoint.Dump()
	loader.jobsQueue = make(chan *job, jobCount)
	loader.firstDataFile = true

	return loader
}

// Start to restore.
func (l *Loader) Restore() error {
	if err := l.prepare(); err != nil {
		log.Errorf("[loader] scan dir[%s] failed, err[%v]", l.cfg.Dir, err)
		return errors.Trace(err)
	}

	if err := l.restoreData(); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (l *Loader) prepareDbFiles(files map[string]struct{}) error {
	for file, _ := range files {
		if !strings.HasSuffix(file, "-schema-create.sql") {
			continue
		}

		idx := strings.Index(file, "-schema-create.sql")
		if idx > 0 {
			db := file[:idx]
			l.dbTables[db] = make(Tables)
		}
	}

	if len(l.dbTables) == 0 {
		return errors.New("invalid mydumper files")
	}

	return nil
}

func (l *Loader) prepareTableFiles(files map[string]struct{}) error {
	for file, _ := range files {
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
		tables, ok := l.dbTables[db]
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
	for file, _ := range files {
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
		tables, ok := l.dbTables[db]
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

func (l *Loader) dispatchJob(job *job) {
	l.jobWg.Add(1)

	l.jobsQueue <- job
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

func (l *Loader) dispatchSQL(file string, schema string, table string, checkExist bool) error {
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

				j := &job{
					sql:                 sql,
					schema:              schema,
					skipConstraintCheck: l.cfg.SkipConstraintCheck == 1,
				}
				if checkExist {
					j.skipConstraintCheck = false
				}
				l.dispatchJob(j)
			}
		}
	}

	return nil
}

func (l *Loader) runWorker(conn *Conn, queue chan *job) {
	defer l.wg.Done()

	count := 0
	batchSize := l.cfg.Batch
	sqls := make([]string, 0, batchSize)
	lastSyncTime := time.Now()

	skipConstraintCheck := true
	for {
		select {
		case job, ok := <-queue:
			if !ok {
				log.Infof("[loader] worker exit")
				l.wg.Done()
				return
			}
			if !job.skipConstraintCheck {
				skipConstraintCheck = false
			}

			if len(sqls) == 0 {
				sqls = append(sqls, fmt.Sprintf("USE %s;", job.schema))
			}
			sqls = append(sqls, job.sql)
			count++
			if count >= batchSize {
				if err := executeSQL(conn, sqls, true, skipConstraintCheck); err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				for i := 0; i < count; i++ {
					l.jobWg.Done()
				}

				count = 0
				sqls = sqls[0:0]
				lastSyncTime = time.Now()

				skipConstraintCheck = true
			}
		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				if err := executeSQL(conn, sqls, true, skipConstraintCheck); err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				for i := 0; i < count; i++ {
					l.jobWg.Done()
				}

				count = 0
				sqls = sqls[0:0]
				lastSyncTime = now

				skipConstraintCheck = true
			}

			time.Sleep(waitTime)
		}
	}
}

func (l *Loader) redoTruncatedContents(schema string, table string, dataFiles []string) error {
	for _, dataFile := range dataFiles {
		// sql -> {db}.{table}.{index}.sql or {db}.{table}.sql
		if !strings.HasSuffix(dataFile, ".sql") {
			continue
		}

		fields := strings.Split(dataFile, ".")
		if len(fields) < 3 {
			continue
		}

		if fields[0] != schema || fields[1] != table {
			continue
		}

		log.Infof("[loader][redo table data sql]%s/%s[start]", l.cfg.Dir, dataFile)

		err := l.restoreDataFile(l.cfg.Dir, dataFile, schema, table, false)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[loader][redo table data sql]%s/%s[end]", l.cfg.Dir, dataFile)
	}

	return nil
}

func (l *Loader) restoreDataFile(path string, dataFile string, schema string, table string, checkExist bool) error {
	log.Infof("[loader][restore table data sql]%s/%s[start]", path, dataFile)

	err := l.dispatchSQL(fmt.Sprintf("%s/%s", path, dataFile), schema, table, checkExist)
	if err != nil {
		return errors.Trace(err)
	}

	l.jobWg.Wait()

	err = l.checkPoint.Save(dataFile)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[loader][saved to checkpoint]")
	log.Infof("[loader][restore table data sql]%s/%s[finished]", path, dataFile)
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

func (l *Loader) restoreData() error {
	var err error
	l.conns, err = createConns(l.cfg.DB, l.cfg.Worker)
	if err != nil {
		return errors.Trace(err)
	}

	l.wg.Add(l.cfg.Worker)
	for i := 0; i < l.cfg.Worker; i++ {
		go l.runWorker(l.conns[i], l.jobsQueue)
	}

	// restore db in sort
	dbs := make([]string, 0, len(l.dbTables))
	for db, _ := range l.dbTables {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		tables := l.dbTables[db]
		// create db
		dbFile := fmt.Sprintf("%s/%s-schema-create.sql", l.cfg.Dir, db)
		log.Infof("[loader][run db schema]%s[start]", dbFile)
		err = l.restoreSchema(l.conns[0], dbFile, "")
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
		for t, _ := range tables {
			tnames = append(tnames, t)
		}
		sort.Strings(tnames)
		for _, table := range tnames {
			// create table
			tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
			log.Infof("[loader][run table schema]%s[start]", tableFile)
			err := l.restoreSchema(l.conns[0], tableFile, db)
			if err != nil {
				if isErrTableExists(err) {
					log.Infof("[loader][table already exists, skip]%s", tableFile)
					if !l.restoreFromCheckpoint {
						l.restoreFromCheckpoint = true
					}
				} else {
					log.Fatalf("run table schema failed - %v", errors.ErrorStack(err))
				}
			}
			log.Infof("[loader][run table schema]%s[finished]", tableFile)

			// restore data in sort for this table
			dataFiles := tables[table]
			sort.Strings(dataFiles)
			var skippedDataFiles []string
			for _, dataFile := range dataFiles {
				_, ok := l.restoredFiles[dataFile]
				if ok {
					skippedDataFiles = append(skippedDataFiles, dataFile)
					log.Infof("[loader][already in restored files, skip]%s", dataFile)
					continue
				}

				if l.firstDataFile && l.restoreFromCheckpoint {
					ok, err = hasUniqIndex(l.conns[0], db, table)
					if err != nil {
						log.Fatalf("check table uniq index failed - %v", errors.ErrorStack(err))
					}

					if !ok { // no unique index, truncate this overlapped table and reload
						err = truncateTable(l.conns[0], db, table)
						if err != nil {
							log.Fatalf("truncate table failed - %s - %s - %v", db, table, errors.ErrorStack(err))
						}
						// We should redo truncated table sql files.
						skippedDataFiles = append(skippedDataFiles, dataFile)
						err = l.redoTruncatedContents(db, table, skippedDataFiles)
						if err != nil {
							log.Fatalf("redo skipped files failed - %v", errors.ErrorStack(err))
						}
					} else {
						// has unique index, use `INSERT IGNORE` for this potential overlapped data file
						err = l.restoreDataFile(l.cfg.Dir, dataFile, db, table, true /*INSERT IGNORE*/)
						if err != nil {
							log.Fatalf("restore datafile [%s] failed - %v", dataFile, errors.ErrorStack(err))
						}
					}
				} else {
					err = l.restoreDataFile(l.cfg.Dir, dataFile, db, table, false /*INSERT*/)
					if err != nil {
						log.Fatalf("run sql file failed - %v", errors.ErrorStack(err))
					}
				}

				if l.firstDataFile {
					l.firstDataFile = false
				}
			}
		}
	}

	log.Infof("All data files has restored, please remove checkpoint file.")

	return nil
}

func (l *Loader) isClosed() bool {
	return l.closed.Get()
}

// Close closes loader.
func (l *Loader) Close() {
	l.Lock()
	defer l.Unlock()

	if l.isClosed() {
		return
	}

	close(l.jobsQueue)

	l.wg.Wait()

	closeConns(l.conns...)

	l.closed.Set(true)
}
