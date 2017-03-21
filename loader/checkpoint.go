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
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"strconv"
)

var (
	FileNumPerBlockSuffix = ".file-num-per-block"
)

// CheckPoint represents checkpoint status
type CheckPoint struct {
	sync.RWMutex
	path            string
	restoredFiles   map[string]map[string]Set
	finishedTables  Set
	FileNumPerBlock int
}

func newCheckPoint(filename string) *CheckPoint {
	cp := &CheckPoint{
		path:            filename,
		restoredFiles:   make(map[string]map[string]Set),
		finishedTables:  make(Set),
		FileNumPerBlock: -1,
	}
	if err := cp.load(); err != nil {
		log.Fatalf("recover from check point failed, %v", err)
	}
	return cp
}

// Get restored data files for table
func (cp *CheckPoint) GetRestoredFiles(db, table string) Set {
	if tables, ok := cp.restoredFiles[db]; ok {
		if restoredFiles, ok := tables[table]; ok {
			return restoredFiles
		}
	}
	return make(Set)
}

// Query if table finished.
func (cp *CheckPoint) IsTableFinished(db, table string) bool {
	key := strings.Join([]string{db, table}, ".")
	if _, ok := cp.finishedTables[key]; ok {
		return true
	}
	return false
}

// Calculate which table has finished and which table partial restored.
func (cp *CheckPoint) CalcProgress(allFiles map[string]Tables2DataFiles) {
	for db, tables := range cp.restoredFiles {
		dbTables, ok := allFiles[db]
		if !ok {
			log.Fatalf("db (%s) not exist in data files, but in checkpoint.", db)
		}

		for table, restoredFiles := range tables {
			files, ok := dbTables[table]
			if !ok {
				log.Fatalf("table (%s) not exist in db (%s) in data files, but in checkpoint", table, db)
			}

			restoredCount := len(restoredFiles)
			totalCount := len(files)

			t := strings.Join([]string{db, table}, ".")
			if restoredCount == totalCount {
				cp.finishedTables[t] = struct{}{}
			} else if restoredCount > totalCount {
				log.Fatalf("restored count greater than total count for table[%v]", table)
			}
		}
	}

	log.Infof("calc checkpoint finished. finished tables (%v)", cp.finishedTables)
}

func (cp *CheckPoint) load() error {
	f, err := os.Open(cp.path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}
	if os.IsNotExist(err) {
		return nil
	}
	defer f.Close()

	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}
		l := strings.TrimSpace(line[:len(line)-1])
		if len(l) == 0 {
			continue
		}

		// read last FileNumPerBlock recorded in checkpoint file
		if cp.FileNumPerBlock < 0 && strings.HasSuffix(l, FileNumPerBlockSuffix) {
			idx := strings.Index(l, FileNumPerBlockSuffix)
			fileNumPerBlock, err := strconv.Atoi(l[:idx])
			if err != nil {
				log.Fatalf("invalid file num per block (%s) in checkpoint file", l)
			}
			cp.FileNumPerBlock = fileNumPerBlock
			continue
		}

		if !strings.HasSuffix(l, ".sql") || strings.HasSuffix(l, "-schema.sql") {
			log.Fatalf("invalid data sql file (%s) in checkpoint file", l)
		}

		idx := strings.Index(l, ".sql")
		fname := l[:idx]
		fields := strings.Split(fname, ".")
		if len(fields) != 2 && len(fields) != 3 {
			log.Fatalf("invalid db table sql file - %s", l)
		}

		// fields[0] -> db name, fields[1] -> table name
		if _, ok := cp.restoredFiles[fields[0]]; !ok {
			cp.restoredFiles[fields[0]] = make(map[string]Set)
		}
		tables := cp.restoredFiles[fields[0]]
		if _, ok := tables[fields[1]]; !ok {
			tables[fields[1]] = make(Set)
		}
		restoredFiles := tables[fields[1]]
		restoredFiles[l] = struct{}{}
	}

	return nil
}

// Save saves current checkpoint status to file
func (cp *CheckPoint) Save(filename string) error {
	cp.Lock()
	defer cp.Unlock()

	// add to checkpoint file
	f, err := os.OpenFile(cp.path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%s\n", filename); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// SaveFileNumPerBlock save file-num-per-block into checkpoint file
func (cp *CheckPoint) SaveFileNumPerBlock(n int) error {
	line := fmt.Sprintf("%d%s", n, FileNumPerBlockSuffix)
	return cp.Save(line)
}
