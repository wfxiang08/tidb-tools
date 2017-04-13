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

package route

import (
	"sync"

	"github.com/juju/errors"
)

// 1. asterisk character (*, also called "star") matches zero or more characters,
//    for example, doc* matches doc and document but not dodo;
//    asterisk character must be in the end of wildcard word,
//    and there is only one asterisk in one wildcard word
// 2. the question mark ? matches exactly one character
const (
	// asterisk [ * ]
	awc = '*'
	// question mark [ ? ]
	qwc = '?'
)

const maxCacheNum = 1024

// Router routes word to target word according to it's pattern word
type Router interface {
	// Insert will inserts one [pattern, target] rule pairs into Router
	Insert(pattern string, target string) error
	// Match will match all items that matched to the origin
	Match(origin string) string
	// Remove will remove the matched rule
	Remove(pattern string) error
	// AllRules will returns all rules
	AllRules() map[string]string
}

type trieRouter struct {
	sync.RWMutex

	cache map[string]string
	root  *node
}

type node struct {
	wcs      map[byte]*item
	awc, qwc *item
}

type item struct {
	next *node
	word string
}

func newNode() *node {
	return &node{wcs: make(map[byte]*item)}
}

// NewTrieRouter returns a trie Router
func NewTrieRouter() Router {
	return &trieRouter{cache: make(map[string]string), root: newNode()}
}

// Insert implements Router's Insert()
func (t *trieRouter) Insert(pattern, target string) error {
	if len(pattern) == 0 || len(target) == 0 {
		return errors.Errorf("pattern %s and target %s can't be empty", pattern, target)
	}

	t.Lock()

	n := t.root
	hadAwc := false
	var entity *item
	for i := range pattern {
		if hadAwc {
			t.Unlock()
			return errors.Errorf("pattern %s is invaild", pattern)
		}

		switch pattern[i] {
		case awc:
			entity = n.awc
			hadAwc = true
		case qwc:
			entity = n.qwc
		default:
			entity = n.wcs[pattern[i]]
		}
		if entity == nil {
			entity = &item{}
			switch pattern[i] {
			case awc:
				n.awc = entity
			case qwc:
				n.qwc = entity
			default:
				n.wcs[pattern[i]] = entity
			}
		}
		if entity.next == nil {
			entity.next = newNode()
		}
		n = entity.next
	}

	if len(entity.word) > 0 && entity.word != target {
		t.Unlock()
		return errors.Errorf("subjects has conflict: had %s, want to insert %s", entity.word, target)
	}

	entity.word = target
	t.Unlock()

	return nil
}

// Match implements Router's Match()
func (t *trieRouter) Match(origin string) string {
	if len(origin) == 0 {
		return ""
	}

	t.RLock()
	target, ok := t.cache[origin]
	t.RUnlock()
	if ok {
		return target
	}

	t.Lock()
	target = t.matchNode(t.root, origin)

	// Add to our cache
	t.cache[origin] = target
	if len(t.cache) > maxCacheNum {
		for origin := range t.cache {
			delete(t.cache, origin)
			break
		}
	}
	t.Unlock()
	return target
}

// Remove implements Router's Remove(), but it do nothing now
func (t *trieRouter) Remove(pattern string) error {
	return nil
}

// AllRules implements Router's AllRules
func (t *trieRouter) AllRules() map[string]string {
	rules := make(map[string]string)
	var characters []byte
	t.RLock()
	t.travel(t.root, characters, rules)
	t.RUnlock()
	return rules
}

func (t *trieRouter) travel(n *node, characters []byte, rules map[string]string) {
	if n == nil {
		return
	}

	if n.awc != nil {
		if len(n.awc.word) > 0 {
			pattern := append(characters, awc)
			rules[string(pattern)] = n.awc.word
		}
	}

	if n.qwc != nil {
		pattern := append(characters, qwc)
		if len(n.qwc.word) > 0 {
			rules[string(pattern)] = n.qwc.word
		}
		t.travel(n.qwc.next, pattern, rules)
	}

	for char, item := range n.wcs {
		pattern := append(characters, char)
		if len(item.word) > 0 {
			rules[string(pattern)] = item.word
		}
		t.travel(item.next, pattern, rules)
	}
}

func (t *trieRouter) matchNode(n *node, origin string) string {
	if n == nil {
		return ""
	}

	var (
		ok     bool
		entity *item
	)
	for i := range origin {
		if n.awc != nil && len(n.awc.word) > 0 {
			return n.awc.word
		}

		if n.qwc != nil {
			if i == len(origin)-1 && len(n.qwc.word) > 0 {
				return n.qwc.word
			}

			target := t.matchNode(n.qwc.next, origin[i+1:])
			if len(target) > 0 {
				return target
			}
		}

		entity, ok = n.wcs[origin[i]]
		if !ok {
			return ""
		}
		n = entity.next
	}

	if entity != nil && len(entity.word) > 0 {
		return entity.word
	}

	if n.awc != nil && len(n.awc.word) > 0 {
		return n.awc.word
	}

	return ""
}
