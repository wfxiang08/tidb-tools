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

	"github.com/google/btree"
	"github.com/juju/errors"
	"github.com/ngaut/log"
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

	meta *LocalMeta

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	files map[string]*btree.BTree

	savedFiles   map[string]struct{}
	skippedFiles map[string]struct{}

	conns []*sql.DB

	jobs []chan *job

	closed sync2.AtomicBool

	firstLoadFile bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewLoader creates a new Loader.
func NewLoader(cfg *Config) *Loader {
	loader := new(Loader)
	loader.cfg = cfg
	loader.closed.Set(false)
	loader.firstLoadFile = true
	loader.meta = newLocalMeta(cfg.Meta)
	loader.files = make(map[string]*btree.BTree)
	loader.savedFiles = make(map[string]struct{})
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

// Start starts Loader.
func (l *Loader) Start() error {
	// check if mydumper dir data exists.
	if !IsDirExists(l.cfg.Dir) {
		return errors.New("empty mydumper dir")
	}

	// collect dir files.
	files := GetDirFiles(l.cfg.Dir)
	_, ok := files["metadata"]
	if !ok {
		return errors.New("invalid mydumper dir, none metadata exists")
	}

	// mydumper file names format
	// db -> {db}-schema-create.sql
	// table -> {db}.{table}-schema.sql
	// sql -> {db}.{table}.{index}.sql or {db}.{table}.sql

	usedFiles := make(map[string]struct{})

	// scan db schema files
	for file, _ := range files {
		idx := strings.Index(file, "-schema-create.sql")
		if idx > 0 {
			db := file[:idx]
			l.files[db] = btree.New(defaultBTreeDegree)
			usedFiles[file] = struct{}{}
		}
	}

	// scan db table schema files
	for file, _ := range files {
		_, ok := usedFiles[file]
		if ok {
			continue
		}

		if !strings.HasSuffix(file, "-schema.sql") {
			continue
		}

		idx := strings.Index(file, "-schema.sql")
		name := file[:idx]
		fields := strings.Split(name, ".")
		if len(fields) < 2 {
			log.Warnf("invalid db table schema file - %s", file)
			continue
		}

		db, table := fields[0], fields[1]
		tables, ok := l.files[db]
		if !ok {
			return errors.Errorf("invalid db table schema file, cannot find db - %s", file)
		}

		it := &item{key: table}
		rit := tables.ReplaceOrInsert(it)
		if rit != nil {
			return errors.Errorf("invalid db table schema file, duplicated item - %s", file)
		}

		usedFiles[file] = struct{}{}
	}

	// scan db table schema files
	for file, _ := range files {
		_, ok := usedFiles[file]
		if ok {
			continue
		}

		if !strings.HasSuffix(file, ".sql") {
			continue
		}

		// ignore view / triggers
		if strings.Index(file, "-schema-view.sql") > 0 || strings.Index(file, "-schema-triggers.sql") > 0 ||
			strings.Index(file, "-schema-post.sql") > 0 {
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
			return errors.Errorf("invalid db table sql file, cannot find db - %s", file)
		}

		key := &item{key: table}
		it := tables.Get(key)
		if it == nil {
			return errors.Errorf("invalid db table sql file, cannot find table - %s", file)
		}

		it.(*item).values = append(it.(*item).values, file)
		usedFiles[file] = struct{}{}
	}

	log.Infof("[files]\n%s", l.contentFiles())

	if len(l.files) == 0 {
		return errors.New("invalid mydumper files")
	}

	err := l.meta.load()
	if err != nil {
		return errors.Trace(err)
	}

	l.savedFiles = l.meta.dump()

	err = l.run()
	if err != nil {
		return errors.Trace(err)
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

func (l *Loader) runSQL(file string, schema string, table string) error {
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
					if l.firstLoadFile {
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

	idx := 0
	count := l.cfg.Batch
	sqls := make([]string, 0, count)
	lastSyncTime := time.Now()

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}

			idx++
			sqls = append(sqls, job.sql)

			if idx >= count {
				err = executeSQL(db, sqls, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				for _ = range sqls {
					l.jobWg.Done()
				}

				idx = 0
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

				idx = 0
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

		err := l.runSQLFile(sqlFile, file, schema, table)
		if err != nil {
			return errors.Trace(err)
		}

		log.Infof("[loader][redo table data sql]%s[end]", sqlFile)
	}

	return nil
}

func (l *Loader) runSQLFile(file string, name string, schema string, table string) error {
	log.Infof("[loader][run table data sql]%s[start]", file)

	err := l.runSQL(file, schema, table)
	if err != nil {
		return errors.Trace(err)
	}

	l.jobWg.Wait()

	err = l.meta.save(name)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("[loader][save meta]%s", l.meta)

	log.Infof("[loader][run table data sql]%s[end]", file)
	return nil
}

func (l *Loader) run() error {
	var err error
	l.conns, err = createDBs(l.cfg.DB, l.cfg.Worker)
	if err != nil {
		return errors.Trace(err)
	}

	l.wg.Add(l.cfg.Worker)
	for i := 0; i < l.cfg.Worker; i++ {
		go l.do(l.conns[i], l.jobs[i])
	}

	l.wg.Add(1)
	go l.printStatus()

	for db, tables := range l.files {
		// run db schema sql
		dbFile := fmt.Sprintf("%s/%s-schema-create.sql", l.cfg.Dir, db)
		log.Infof("[loader][run db schema]%s[start]", dbFile)
		err = l.runSchema(l.conns[0], dbFile, "")
		if err != nil {
			log.Warnf("run db schema failed - %v", errors.ErrorStack(err))
		}
		log.Infof("[loader][run db schema]%s[end]", dbFile)

		tables.Ascend(func(i btree.Item) bool {
			table := i.(*item).key

			// run table schema sql
			tableFile := fmt.Sprintf("%s/%s.%s-schema.sql", l.cfg.Dir, db, table)
			log.Infof("[loader][run table schema]%s[start]", tableFile)
			err := l.runSchema(l.conns[0], tableFile, db)
			if err != nil {
				log.Warnf("run table schema failed - %v", errors.ErrorStack(err))
			}
			log.Infof("[loader][run table schema]%s[end]", tableFile)

			sqls := i.(*item).values
			sort.Strings(sqls)

			for _, sql := range sqls {
				// run table data sql
				sqlFile := fmt.Sprintf("%s/%s", l.cfg.Dir, sql)

				_, ok := l.savedFiles[sql]
				if ok {
					l.skippedFiles[sql] = struct{}{}
					log.Infof("[loader][already in saves files, skip]%s", sql)
					continue
				}

				// check if the table has uniq index, if not, we should truncate table first.
				if l.firstLoadFile {
					ok, err = checkTableUniqIndex(l.conns[0], db, table)
					if err != nil {
						log.Fatalf("check table uniq index failed - %v", errors.ErrorStack(err))
					}

					if !ok {
						err = truncateTable(l.conns[0], db, table)
						if err != nil {
							log.Fatalf("truncate table failed - %s - %s - %v", db, table, errors.ErrorStack(err))
						}

						l.firstLoadFile = false

						// We should redo truncated table sql files in saved map.
						err = l.redoSkippedFiles(db, table)
						if err != nil {
							log.Fatalf("redo skipped files failed - %v", errors.ErrorStack(err))
						}
					}
				}

				err = l.runSQLFile(sqlFile, sql, db, table)
				if err != nil {
					log.Fatalf("run sql file failed - %v", errors.ErrorStack(err))
				}

				if l.firstLoadFile {
					l.firstLoadFile = false
				}
			}

			return true
		})
	}

	return nil
}

func (l *Loader) printStatus() {
	defer l.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-timer.C:
			// TODO: print more informations.
			log.Infof("[loader meta]%s", l.meta)
		}
	}
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
