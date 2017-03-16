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
	"sort"
)

// CheckPoint represents checkpoint status
type CheckPoint struct {
	sync.RWMutex
	path               string
	restoredFilesIndex map[string]Tables2DataFiles
	restoredFiles      map[string]struct{}

	// table finished
	FinishedTables map[string]struct{}
	// table partial restored
	PartialRestoredTables map[string]int
}

func newCheckPoint(filename string) *CheckPoint {
	cp := &CheckPoint{
		path:                  filename,
		restoredFilesIndex:    make(map[string]Tables2DataFiles),
		restoredFiles:         make(map[string]struct{}),
		FinishedTables:        make(map[string]struct{}),
		PartialRestoredTables: make(map[string]int),
	}
	if err := cp.load(); err != nil {
		log.Fatalf("recover from check point failed, %v", err)
	}
	return cp
}

func (cp *CheckPoint) Calc(allFiles map[string]Tables2DataFiles) {
	if len(cp.restoredFiles) == 0 {
		return
	}

	for db, tables := range cp.restoredFilesIndex {
		dbTables, ok := allFiles[db]
		if !ok {
			log.Fatalf("db (%s) not exist in data files, but in checkpoint.", db)
		}

		for table, restoredFiles := range tables {
			files, ok := dbTables[table]
			if !ok {
				log.Fatalf("table (%s) not exist in db (%s) in data files, but in checkpoint", table, db)
			}
			// compare restored files and data files for this table
			sort.Strings(files)
			sort.Strings(restoredFiles)
			restoredCount := len(restoredFiles)
			totalCount := len(files)

			t := strings.Join([]string{db, table}, ".")
			if restoredCount == totalCount {
				cp.FinishedTables[t] = struct{}{}
			} else if restoredCount < totalCount {
				// check point for this table
				cp.PartialRestoredTables[t] = restoredCount
			} else {
				log.Fatalf("restored count (%d) gt total count (%d) for table (%s)", restoredCount, totalCount, t)
			}
		}
	}
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

		if !strings.HasSuffix(l, ".sql") || strings.HasSuffix(l, "-schema.sql") {
			log.Fatalf("invalid sql file (%s) in checkpoint file", l)
		}

		idx := strings.Index(l, ".sql")
		fname := l[:idx]
		fields := strings.Split(fname, ".")
		if len(fields) != 2 && len(fields) != 3 {
			log.Fatalf("invalid db table sql file - %s", l)
		}

		// fields[0] -> db name, fields[1] -> table name
		if _, ok := cp.restoredFilesIndex[fields[0]]; !ok {
			cp.restoredFilesIndex[fields[0]] = make(Tables2DataFiles)
		}
		tables := cp.restoredFilesIndex[fields[0]]
		if _, ok := tables[fields[1]]; !ok {
			tables[fields[1]] = make(DataFiles, 0, 16)
		}
		// dataFiles contains data files has restored for this table
		dataFiles := tables[fields[1]]
		dataFiles = append(dataFiles, l)
		cp.restoredFiles[l] = struct{}{}
	}

	log.Infof("calc checkpoint finished. finished(%v), partial(%v)", cp.FinishedTables, cp.PartialRestoredTables)

	return nil
}

// Save saves current checkpoint status to file
func (cp *CheckPoint) Save(filename string) error {
	cp.Lock()
	defer cp.Unlock()

	cp.restoredFiles[filename] = struct{}{}

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
