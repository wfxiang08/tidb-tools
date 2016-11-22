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
	"database/sql"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	tidb "github.com/pingcap/tidb/mysql"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

type job struct {
	sql string
}

func newJob(sql string) *job {
	return &job{sql: sql}
}

var (
	defaultBTreeDegree = 64
	jobCount           = 1000
	maxRetryCount      = 100

	waitTime    = 10 * time.Millisecond
	maxWaitTime = 3 * time.Second
	statusTime  = 30 * time.Second
)

// Loader can load your mydumper data into MySQL database.
type Loader struct {
	sync.Mutex

	cfg *Config

	checkPoint *CheckPoint

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	files map[string]*btree.BTree

	restoredFiles map[string]struct{}
	skippedFiles  map[string]struct{}

	conns []*sql.DB

	jobs []chan *job

	closed sync2.AtomicBool

	firstFileAfterCheckpoint          bool
	restoreFromCheckpoint bool
	ctx                   context.Context
	cancel                context.CancelFunc
}

// NewLoader creates a new Loader.
func NewLoader(cfg *Config) *Loader {
	loader := new(Loader)
	loader.cfg = cfg
	loader.closed.Set(false)
	loader.checkPoint = newCheckPoint(cfg.CheckPoint)
	loader.firstFileAfterCheckpoint = false
	loader.restoreFromCheckpoint = loader.checkPoint.IsRestoreFromLastCheckPoint()
	loader.files = make(map[string]*btree.BTree)
	loader.restoredFiles = loader.checkPoint.Dump()
	loader.skippedFiles = make(map[string]struct{})
	loader.jobs = newJobChans(cfg.Worker)
	loader.ctx, loader.cancel = context.WithCancel(context.Background())
	return loader
}

func newJobChans(count int) []chan *job {
	jobs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobs = append(jobs, make(chan *job, jobCount))
	}

	return jobs
}

func closeJobChans(jobs []chan *job) {
	for _, ch := range jobs {
		close(ch)
	}
}

func (l *Loader) contentFiles() string {
	var content []byte
	for key, value := range l.files {
		content = append(content, []byte(fmt.Sprintf("[db]%s\n", key))...)
		value.Ascend(func(i btree.Item) bool {
			for _, file := range i.(*item).values {
				content = append(content, []byte(fmt.Sprintf("[table]%s[sql]%s\n", i.(*item).key, file))...)
			}

			return true
		})
	}

	return string(content)
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

	usedFiles := make(map[string]struct{})

	// Sql file for create db
	for file, _ := range files {
		idx := strings.Index(file, "-schema-create.sql")
		if idx > 0 {
			db := file[:idx]
			l.files[db] = btree.New(defaultBTreeDegree)
			usedFiles[file] = struct{}{}
		}
	}

	// Sql file for create table
	for file, _ := range files {
		_, ok := usedFiles[file]
		if ok || !strings.HasSuffix(file, "-schema.sql") {
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
		tables, ok := l.files[db]
		if !ok {
			return errors.Errorf("invalid table schema file, cannot find db - %s", file)
		}

		it := &item{key: table}
		rit := tables.ReplaceOrInsert(it)
		if rit != nil {
			return errors.Errorf("invalid table schema file, duplicated item - %s", file)
		}

		usedFiles[file] = struct{}{}
	}

	// Sql file for restore data
	for file, _ := range files {
		_, ok := usedFiles[file]
		if ok || !strings.HasSuffix(file, ".sql") {
			continue
		}

		// ignore view / triggers
		if strings.Index(file, "-schema-view.sql") >= 0 || strings.Index(file, "-schema-triggers.sql") >= 0 ||
			strings.Index(file, "-schema-post.sql") >= 0 {
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
		tables, ok := l.files[db]
		if !ok {
			return errors.Errorf("invalid data sql file, cannot find db - %s", file)
		}

		key := &item{key: table}
		it := tables.Get(key)
		if it == nil {
			return errors.Errorf("invalid data sql file, cannot find table - %s", file)
		}

		it.(*item).values = append(it.(*item).values, file)
		usedFiles[file] = struct{}{}
	}

	if len(l.files) == 0 {
		return errors.New("invalid mydumper files")
	}

	return nil
}

func (l *Loader) addJob(job *job) error {
	l.jobWg.Add(1)

	idx := int(genHashKey(job.sql)) % l.cfg.Worker
	l.jobs[idx] <- job

	return nil
}

func (l *Loader) runSchema(db *sql.DB, file string, schema string) error {
	f, err := os.Open(file)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	var data []byte
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
				err = executeSQL(db, sqls, false)
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

	var data []byte
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

				listIdx := strings.Index(query, "VALUES")
				if listIdx <= 0 {
					return errors.Errorf("[invalid insert sql][sql]%s", query)
				}

				values := query[listIdx+len("VALUES"):]
				values = strings.TrimRight(values, ";")
				values = strings.TrimSpace(values)
				if values[0] != '(' && values[len(values)-1] != ')' {
					return errors.Errorf("[invalid insert sql][sql]%s", query)
				}

				values = values[1 : len(values)-1]
				fields := strings.Split(values, "),(")
				if len(fields) == 0 {
					return errors.Errorf("[invalid insert sql][sql]%s", query)
				}

				for i := range fields {
					var sql string
					if checkExist {
						sql = fmt.Sprintf("insert ignore into %s.%s values (%s);", schema, table, fields[i])
					} else {
						sql = fmt.Sprintf("insert into %s.%s values (%s);", schema, table, fields[i])
					}

					job := newJob(sql)
					l.addJob(job)
					fmt.Printf("[run sql][%d]%s\n", i, sql)
				}
			}
		}
	}

	return nil
}

func (l *Loader) do(db *sql.DB, jobChan chan *job) {
	defer l.wg.Done()

	count := 0
	batchSize := l.cfg.Batch
	sqls := make([]string, 0, batchSize)
	lastSyncTime := time.Now()

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				log.Infof("[loader] worker exit")
				return
			}

			count++
			sqls = append(sqls, job.sql)

			if count >= batchSize {
				err = executeSQL(db, sqls, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				for _ = range sqls {
					l.jobWg.Done()
				}

				count = 0
				sqls = sqls[0:0]
				lastSyncTime = time.Now()
			}
		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQL(db, sqls, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				for _ = range sqls {
					l.jobWg.Done()
				}

				count = 0
				sqls = sqls[0:0]
				lastSyncTime = now
			}

			time.Sleep(waitTime)
		}
	}
}

func (l *Loader) redoSkippedFiles(schema string, table string) error {
	for file := range l.skippedFiles {
		// sql -> {db}.{table}.{index}.sql or {db}.{table}.sql
		if !strings.HasSuffix(file, ".sql") {
			continue
		}

		fields := strings.Split(file, ".")
		if len(fields) < 3 {
			continue
		}

		if fields[0] != schema || fields[1] != table {
			continue
		}

		sqlFile := fmt.Sprintf("%s/%s", l.cfg.Dir, file)

		log.Infof("[loader][redo table data sql]%s[start]", sqlFile)

		err := l.restoreDataFile(sqlFile, file, schema, table, false)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[loader][redo table data sql]%s[end]", sqlFile)
	}

	return nil
}

func (l *Loader) restoreDataFile(abDataFile string, dataFile string, schema string, table string, checkExist bool) error {
	log.Infof("[loader][restore table data sql]%s[start]", abDataFile)

	err := l.dispatchSQL(abDataFile, schema, table, checkExist)
	if err != nil {
		return errors.Trace(err)
	}

	l.jobWg.Wait()

	err = l.checkPoint.Save(dataFile)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[loader][saved to checkpoint]")
	log.Infof("[loader][restore table data sql]%s[finished]", abDataFile)
	return nil
}

func (l *Loader) restoreData() error {
	var err error
	l.conns, err = connectToDB(l.cfg.DB, l.cfg.Worker)
	if err != nil {
		return errors.Trace(err)
	}

	l.wg.Add(l.cfg.Worker)
	for i := 0; i < l.cfg.Worker; i++ {
		go l.do(l.conns[i], l.jobs[i])
	}

	for db, tables := range l.files {
		// create db
		dbFile := fmt.Sprintf("%s/%s-schema-create.sql", l.cfg.Dir, db)
		log.Infof("[loader][run db schema]%s[start]", dbFile)
		err = l.runSchema(l.conns[0], dbFile, "")
		if err != nil {
			log.Warnf("run db schema failed - %v", errors.ErrorStack(err))
		}
		log.Infof("[loader][run db schema]%s[end]", dbFile)

		tables.Ascend(func(i btree.Item) bool {
			table := i.(*item).key

			// create table
			tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
			log.Infof("[loader][run table schema]%s[start]", tableFile)
			err := l.runSchema(l.conns[0], tableFile, db)
			if err != nil {
				log.Warnf("run table schema failed - %v", errors.ErrorStack(err))

				if !l.restoreFromCheckpoint && err.(*mysql.MySQLError).Number == tidb.ErrTableExists {
					l.restoreFromCheckpoint = true
				}
			}
			log.Infof("[loader][run table schema]%s[end]", tableFile)

			dataFiles := i.(*item).values
			sort.Strings(dataFiles)

			// restore data for this table
			for _, dataFile := range dataFiles {
				abDataFile := fmt.Sprintf("%s/%s", l.cfg.Dir, dataFile)

				_, ok := l.restoredFiles[dataFile]
				if ok {
					l.skippedFiles[dataFile] = struct{}{}
					log.Infof("[loader][already in saves files, skip]%s", dataFile)
					continue
				}

				if l.restoreFromCheckpoint {
					ok, err = checkTableUniqIndex(l.conns[0], db, table)
					if err != nil {
						log.Fatalf("check table uniq index failed - %v", errors.ErrorStack(err))
					}

					if !ok { // no unique index
						err = truncateTable(l.conns[0], db, table)
						if err != nil {
							log.Fatalf("truncate table failed - %s - %s - %v", db, table, errors.ErrorStack(err))
						}

						l.restoreFromCheckpoint = false

						// We should redo truncated table sql files in saved map.
						err = l.redoSkippedFiles(db, table)
						if err != nil {
							log.Fatalf("redo skipped files failed - %v", errors.ErrorStack(err))
						}
					} else {
						// has unique index, use `INSERT ... IGNORE`
						l.restoreDataFile(abDataFile, dataFile, db, table, true)
					}

				} else {
					err = l.restoreDataFile(abDataFile, dataFile, db, table, false)
					if err != nil {
						log.Fatalf("run sql file failed - %v", errors.ErrorStack(err))
					}
				}

				if l.restoreFromCheckpoint {
					l.restoreFromCheckpoint = false
				}
			}

			l.skippedFiles = make

			return true
		})
	}

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

	l.cancel()

	closeJobChans(l.jobs)

	l.wg.Wait()

	closeDBs(l.conns...)

	l.closed.Set(true)
}
