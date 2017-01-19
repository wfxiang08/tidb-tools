package main

import (
	"regexp"
	"strings"

	"github.com/ngaut/log"
)

var (
	defaultIgnoreDB = "mysql"
	triggerRegex    = regexp.MustCompile(`(^CREATE (DEFINER=\S+ )*TRIGGER)`)
)

// whiteFilter whitelist filtering
func (s *Syncer) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName
	if len(s.cfg.DoTables) == 0 && len(s.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if s.matchTable(s.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

// blackFilter blacklist filtering
func (s *Syncer) blackFilter(stbs []TableName) []TableName {
	var tbs []TableName
	for _, tb := range stbs {
		if s.matchTable(s.cfg.IgnoreTables, tb) {
			continue
		}
		if s.matchDB(s.cfg.IgnoreDBs, tb.Schema) {
			continue
		}
		tbs = append(tbs, tb)
	}
	return tbs
}

func (s *Syncer) skipQueryEvent(sql string, schema string) bool {
	sql = strings.ToUpper(sql)

	// For mariadb, for query event, like `# Dumm`
	// But i don't know what is the meaning of this event.
	if strings.HasPrefix(sql, "#") {
		return true
	}

	if strings.HasPrefix(sql, "GRANT REPLICATION SLAVE ON") {
		return true
	}

	if strings.HasPrefix(sql, "GRANT ALL PRIVILEGES ON") {
		return true
	}

	if strings.HasPrefix(sql, "FLUSH PRIVILEGES") {
		return true
	}

	if strings.HasPrefix(sql, "SAVEPOINT") {
		return true
	}

	if strings.HasPrefix(sql, "OPTIMIZE TABLE") {
		return true
	}

	if strings.HasPrefix(sql, "DROP TRIGGER") {
		return true
	}

	if triggerRegex.FindStringIndex(sql) != nil {
		return true
	}
	if schema == defaultIgnoreDB {
		return true
	}

	return false
}

// skipRowEvent first whitelist filtering and then blacklist filtering
func (s *Syncer) skipRowEvent(schema string, table string) bool {
	if schema == defaultIgnoreDB {
		return true
	}
	tbs := []TableName{
		{
			Schema: strings.ToLower(schema),
			Name:   strings.ToLower(table),
		},
	}
	tbs = s.whiteFilter(tbs)
	tbs = s.blackFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

// skipQueryDDL first whitelist filtering and then blacklist filtering
func (s *Syncer) skipQueryDDL(sql string, schema string) bool {
	tb, err := parserDDLTableName(sql)
	if err != nil {
		log.Warnf("[get table failure]:%s %s", sql, err)
	}
	if tb.Schema == "" {
		tb.Schema = schema
	}
	if tb.Schema == defaultIgnoreDB {
		return true
	}
	if err == nil {
		tbs := []TableName{tb}
		tbs = s.whiteFilter(tbs)
		tbs = s.blackFilter(tbs)
		if len(tbs) == 0 {
			return true
		}
		return false
	}
	return false
}

func (s *Syncer) matchString(pattern string, t string) bool {
	if re, ok := s.reMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (s *Syncer) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if s.matchString(b, a) {
			return true
		}
	}
	return false
}

func (s *Syncer) matchTable(patternTBS []TableName, tb TableName) bool {
	for _, ptb := range patternTBS {
		if s.matchString(ptb.Name, tb.Name) && s.matchString(ptb.Schema, tb.Schema) {
			return true
		}

		//create database or drop database
		if tb.Name == "" {
			if s.matchString(tb.Schema, ptb.Schema) {
				return true
			}
		}
	}

	return false
}
