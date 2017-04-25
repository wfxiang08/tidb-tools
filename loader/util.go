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
	"path/filepath"
	"strings"
)

// IsFileExists checks if file exists.
func IsFileExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if f.IsDir() {
		return false
	}

	return true
}

// IsDirExists checks if dir exists.
func IsDirExists(name string) bool {
	f, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}

	if !f.IsDir() {
		return false
	}

	return true
}

// CollectDirFiles gets files in path
func CollectDirFiles(path string) map[string]struct{} {
	files := make(map[string]struct{})
	filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		if f == nil {
			return nil
		}

		if f.IsDir() {
			return nil
		}

		name := strings.TrimSpace(f.Name())
		files[name] = struct{}{}
		return nil
	})

	return files
}

// SQLReplace works like strings.Replace but only supports one replacement.
// It uses backquote pairs to quote the old and new word.
func SQLReplace(s, old, new string) string {
	old = backquote(old)
	new = backquote(new)
	return strings.Replace(s, old, new, 1)
}

func backquote(s string) string {
	buf := bytes.Buffer{}
	buf.WriteByte('`')
	buf.WriteString(s)
	buf.WriteByte('`')
	return buf.String()
}
