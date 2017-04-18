// Copyright 2017 PingCAP, Inc.
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
	asterisk = '*'
	// question mark [ ? ]
	question = '?'
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

// ref https://en.wikipedia.org/wiki/Trie
type trieRouter struct {
	sync.RWMutex

	cache map[string]string
	root  *node

	isEmpty bool
}

type node struct {
	characters         map[byte]*item
	asterisk, question *item
}

type item struct {
	next    *node
	literal string
}

func newNode() *node {
	return &node{characters: make(map[byte]*item)}
}

// NewTrieRouter returns a trie Router
func NewTrieRouter() Router {
	return &trieRouter{cache: make(map[string]string), root: newNode(), isEmpty: true}
}

// Insert implements Router's Insert()
func (t *trieRouter) Insert(pattern, target string) error {
	if len(pattern) == 0 || len(target) == 0 {
		return errors.Errorf("pattern %s and target %s can't be empty", pattern, target)
	}
	if pattern[0] == asterisk {
		return errors.Errorf("unsupported pattern %s", pattern)
	}

	t.Lock()

	n := t.root
	hasAsterisk := false
	var entity *item
	for i := range pattern {
		if hasAsterisk {
			t.Unlock()
			return errors.Errorf("pattern %s is invaild", pattern)
		}

		switch pattern[i] {
		case asterisk:
			entity = n.asterisk
			hasAsterisk = true
		case question:
			entity = n.question
		default:
			entity = n.characters[pattern[i]]
		}
		if entity == nil {
			entity = &item{}
			switch pattern[i] {
			case asterisk:
				n.asterisk = entity
			case question:
				n.question = entity
			default:
				n.characters[pattern[i]] = entity
			}
		}
		if entity.next == nil {
			entity.next = newNode()
		}
		n = entity.next
	}

	if len(entity.literal) > 0 && entity.literal != target {
		t.Unlock()
		return errors.Errorf("subjects has conflict: had %s, want to insert %s", entity.literal, target)
	}

	t.isEmpty = false
	entity.literal = target
	t.Unlock()

	return nil
}

// Match implements Router's Match()
func (t *trieRouter) Match(s string) string {
	if len(s) == 0 {
		return ""
	}

	t.RLock()
	if t.isEmpty {
		t.RUnlock()
		return ""
	}

	target, ok := t.cache[s]
	t.RUnlock()
	if ok {
		return target
	}

	t.Lock()
	target = t.matchNode(t.root, s)

	// Add to our cache
	t.cache[s] = target
	if len(t.cache) > maxCacheNum {
		for literal := range t.cache {
			delete(t.cache, literal)
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

	if n.asterisk != nil {
		if len(n.asterisk.literal) > 0 {
			pattern := append(characters, asterisk)
			rules[string(pattern)] = n.asterisk.literal
		}
	}

	if n.question != nil {
		pattern := append(characters, question)
		if len(n.question.literal) > 0 {
			rules[string(pattern)] = n.question.literal
		}
		t.travel(n.question.next, pattern, rules)
	}

	for char, item := range n.characters {
		pattern := append(characters, char)
		if len(item.literal) > 0 {
			rules[string(pattern)] = item.literal
		}
		t.travel(item.next, pattern, rules)
	}
}

func (t *trieRouter) matchNode(n *node, s string) string {
	if n == nil {
		return ""
	}

	var (
		ok     bool
		entity *item
	)
	for i := range s {
		if n.asterisk != nil && len(n.asterisk.literal) > 0 {
			return n.asterisk.literal
		}

		if n.question != nil {
			if i == len(s)-1 && len(n.question.literal) > 0 {
				return n.question.literal
			}

			target := t.matchNode(n.question.next, s[i+1:])
			if len(target) > 0 {
				return target
			}
		}

		entity, ok = n.characters[s[i]]
		if !ok {
			return ""
		}
		n = entity.next
	}

	if entity != nil && len(entity.literal) > 0 {
		return entity.literal
	}

	if n.asterisk != nil && len(n.asterisk.literal) > 0 {
		return n.asterisk.literal
	}

	return ""
}
