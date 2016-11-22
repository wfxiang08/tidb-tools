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
	"os"
	"sync"

	"github.com/juju/errors"
	"bufio"
	"io"
	"strings"
	"fmt"
)

type CheckPoint struct {
	sync.RWMutex
	path                      string
	restoredFiles             map[string]struct{}
	restoreFromLastCheckPoint bool
}

func newCheckPoint(filename string) *CheckPoint {
	cp := &CheckPoint{path: filename, restoreFromLastCheckPoint: false}
	if err := cp.load(); err != nil {
		panic(fmt.Sprintf("recover from check point failed, %v", err))
	}
	return cp
}

func (cp *CheckPoint) IsRestoreFromLastCheckPoint() bool {
	return cp.restoreFromLastCheckPoint
}

func (cp *CheckPoint) load() error {
	f, err := os.Open(cp.path)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer f.Close()

	cp.restoreFromLastCheckPoint = true
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		} else {
			l := strings.TrimSpace(line[:len(line)-1])
			if len(l) == 0 {
				continue
			}

			cp.restoredFiles[string(l)] = struct{}{}
		}
	}

	return nil
}

func (cp *CheckPoint) Save(filename string) error {
	cp.Lock()
	defer cp.Unlock()

	cp.restoredFiles[filename] = struct{}{}

	// add to checkpoint file
	f, err := os.OpenFile(cp.path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	// seek to end, and append
	n, _ := f.Seek(0, os.SEEK_END)
	if _, err := f.WriteAt([]byte(filename + '\n'), n); err != nil {
		return err
	}

	return nil
}

func (cp *CheckPoint) Dump() map[string]struct{} {
	cp.RLock()
	defer cp.RUnlock()

	m := make(map[string]struct{})
	for file, _ := range cp.restoredFiles {
		m[file] = struct{}{}
	}

	return m
}

