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
	"os"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go/ioutil2"
)

// LocalMeta is local meta struct.
type LocalMeta struct {
	sync.RWMutex

	name string

	Files []string `toml:"files" json:"files"`
}

func newLocalMeta(name string) *LocalMeta {
	return &LocalMeta{name: name}
}

func (lm *LocalMeta) load() error {
	file, err := os.Open(lm.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, lm)
	return errors.Trace(err)
}

func (lm *LocalMeta) save(file string) error {
	lm.Lock()
	defer lm.Unlock()

	lm.Files = append(lm.Files, file)

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("loader save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(lm.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("loader save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func (lm *LocalMeta) dump() map[string]struct{} {
	lm.RLock()
	defer lm.RUnlock()

	m := make(map[string]struct{})
	for _, file := range lm.Files {
		m[file] = struct{}{}
	}

	return m
}

func (lm *LocalMeta) String() string {
	lm.RLock()
	defer lm.RUnlock()

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("[print local meta error]%v", err)
	}

	return buf.String()
}
