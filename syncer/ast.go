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

package main

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/types"
)

// FIXME: tidb's AST is error-some to handle more condition
func columnDefToSQL(colDef *ast.ColumnDef) string {
	typeDef := colDef.Tp.String()
	sql := fmt.Sprintf("%s %s", columnNameToSQL(colDef.Name), typeDef)
	for _, opt := range colDef.Options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			sql += " NOT NULL"
		case ast.ColumnOptionNull:
			sql += " NULL"
		case ast.ColumnOptionDefaultValue:
			sql += " DEFAULT (TODO)"
			panic("not implemented yet")
		case ast.ColumnOptionAutoIncrement:
			sql += " AUTO_INCREMENT"
		case ast.ColumnOptionUniqKey:
			sql += " UNIQUE KEY"
		case ast.ColumnOptionKey:
			sql += " KEY"
		case ast.ColumnOptionUniq:
			sql += " UNIQUE"
		case ast.ColumnOptionIndex:
			sql += " INDEX"
		case ast.ColumnOptionUniqIndex:
			sql += " UNIQUE INDEX"
		case ast.ColumnOptionPrimaryKey:
			sql += " PRIMARY KEY"
		case ast.ColumnOptionComment:
			sql += " COMMENT 'TODO: not implemented in syncer yet'"
		case ast.ColumnOptionOnUpdate: // For Timestamp and Datetime only.
			sql += "ON UPDATE (TODO)"
			panic("not implemented yet")
		case ast.ColumnOptionFulltext:
			panic("not implemented yet")
		default:
			panic("not implemented yet")
		}
	}
	return sql
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func tableNameToSQL(tbl *ast.TableName) string {
	sql := ""
	if tbl.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", tbl.Schema.O)
	}
	sql += fmt.Sprintf("`%s`", tbl.Name.O)
	return sql
}

func columnNameToSQL(name *ast.ColumnName) string {
	sql := ""
	if name.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Schema.O))
	}
	if name.Table.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Table.O))
	}
	sql += fmt.Sprintf("`%s`", escapeName(name.Name.O))
	return sql
}

func indexColNameToSQL(name *ast.IndexColName) string {
	sql := columnNameToSQL(name.Column)
	if name.Length != types.UnspecifiedLength {
		sql += fmt.Sprintf(" (%d)", name.Length)
	}
	return sql
}

func constraintKeysToSQL(keys []*ast.IndexColName) string {
	if len(keys) == 0 {
		panic("unreachable")
	}
	sql := ""
	for i, indexColName := range keys {
		if i == 0 {
			sql += "("
		}
		sql += indexColNameToSQL(indexColName)
		if i != len(keys)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

func referenceDefToSQL(refer *ast.ReferenceDef) string {
	sql := fmt.Sprintf("%s ", tableNameToSQL(refer.Table))
	sql += constraintKeysToSQL(refer.IndexColNames)
	if refer.OnDelete != nil {
		sql += fmt.Sprintf(" ON DELETE %s", refer.OnDelete.ReferOpt)
	}
	if refer.OnUpdate != nil {
		sql += fmt.Sprintf(" ON UPDATE %s", refer.OnUpdate.ReferOpt)
	}
	return sql
}

func constraintToSQL(constraint *ast.Constraint) string {
	sql := ""
	switch constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex:
		sql += "INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)

	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		sql += "UNIQUE INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)

	case ast.ConstraintForeignKey:
		sql += "FOREIGN KEY "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)
		sql += " REFERENCES "
		panic("not implemented yet")

	case ast.ConstraintPrimaryKey:
		sql += "PRIMARY KEY "
		sql += constraintKeysToSQL(constraint.Keys)

	case ast.ConstraintFulltext:
		fallthrough

	default:
		panic("not implemented yet")
	}
	return sql
}

func alterTableSpecToSQL(spec *ast.AlterTableSpec) string {
	sql := ""
	switch spec.Tp {
	case ast.AlterTableAddColumn:
		sql += fmt.Sprintf("ADD COLUMN %s", columnDefToSQL(spec.NewColumn))
		if spec.Position != nil {
			switch spec.Position.Tp {
			case ast.ColumnPositionNone:
			case ast.ColumnPositionFirst:
				colName := spec.Position.RelativeColumn.Name.O
				sql += fmt.Sprintf(" FIRST `%s`", escapeName(colName))
			case ast.ColumnPositionAfter:
				colName := spec.Position.RelativeColumn.Name.O
				sql += fmt.Sprintf(" AFTER `%s`", escapeName(colName))
			default:
				panic("unreachable")
			}
		}

	case ast.AlterTableDropColumn:
		sql += fmt.Sprintf("DROP COLUMN %s", columnNameToSQL(spec.OldColumnName))

	case ast.AlterTableDropIndex:
		sql += fmt.Sprintf("DROP INDEX `%s`", escapeName(spec.Name))

	case ast.AlterTableAddConstraint:
		sql += "ADD CONSTRAINT "
		if spec.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(spec.Name))
		}
		sql += constraintToSQL(spec.Constraint)

	case ast.AlterTableDropForeignKey:
		sql += fmt.Sprintf("DROP FOREIGN KEY `%s`", escapeName(spec.Name))

	case ast.AlterTableModifyColumn:
		sql += "MODIFY COLUMN "
		sql += columnDefToSQL(spec.NewColumn)

	case ast.AlterTableChangeColumn:
		sql += "CHANGE COLUMN "
		sql += fmt.Sprintf("%s %s",
			columnNameToSQL(spec.OldColumnName),
			columnDefToSQL(spec.NewColumn))

	case ast.AlterTableRenameTable:
		sql += fmt.Sprintf("RENAME TO %s", tableNameToSQL(spec.NewTable))

	case ast.AlterTableDropPrimaryKey:
		fallthrough
	default:
	}
	return sql
}

func alterTableStmtToSQL(stmt *ast.AlterTableStmt) string {
	sql := fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(stmt.Table))
	for i, spec := range stmt.Specs {
		if i != 0 {
			sql += ", "
		}
		sql += alterTableSpecToSQL(spec)
	}
	return sql
}
