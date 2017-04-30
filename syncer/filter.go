package main

import (
	"regexp"
	"strings"

	"github.com/ngaut/log"
)

var (
	defaultIgnoreDB = "mysql"
	triggerRegex    = regexp.MustCompile(`(^CREATE (DEFINER=\S+ )*TRIGGER)`)
	skipSQLs        = []string{
		// For mariadb, for query event, like `# Dumm`
		// But i don't know what is the meaning of this event.
		"^#",
		"^GRANT",
		"^REVOKE",
		"^FLUSH\\s+PRIVILEGES",
		"^SAVEPOINT",
		"^OPTIMIZE\\s+TABLE",
		"^DROP\\s+TRIGGER",
		"^CREATE\\s+VIEW",
		"^DROP\\s+VIEW",
		"^CREATE\\s+ALGORITHM",
		"^DROP\\s+USER",
		"^ALTER\\s+USER",
		"^CREATE\\s+USER",
	}
	skipPatterns *regexp.Regexp
)

func init() {
	skipPatterns = regexp.MustCompile("(?i)" + strings.Join(skipSQLs, "|"))
}

// whiteFilter whitelist filtering
func (s *Syncer) whiteFilter(stbs []TableName) []TableName {
	var tbs []TableName

	// 如果没有指定，则直接跳过
	if len(s.cfg.DoTables) == 0 && len(s.cfg.DoDBs) == 0 {
		return stbs
	}

	for _, tb := range stbs {
		// 这是什么逻辑呢?
		// tbs是否包含重复的选项呢?
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
	if skipPatterns.FindStringIndex(sql) != nil {
		return true
	}

	for _, skipSQL := range s.cfg.SkipSQLs {
		if strings.HasPrefix(strings.ToUpper(sql), strings.ToUpper(skipSQL)) {
			return true
		}
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
	// mysql中的很多数据，可能涉及到授权等，这里不同步
	if schema == defaultIgnoreDB {
		return true
	}
	tbs := []TableName{
		{
			Schema: strings.ToLower(schema),
			Name:   strings.ToLower(table),
		},
	}

	// 如何做Filter呢?
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
	if re, ok := s.patternMap[pattern]; ok {
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

		// create database or drop database
		if tb.Name == "" {
			if s.matchString(tb.Schema, ptb.Schema) {
				return true
			}
		}
	}

	return false
}
