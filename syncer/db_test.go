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
	"errors"

	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	tmysql "github.com/pingcap/tidb/mysql"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct {
}

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (s *testDBSuite) TestIsRetryableError(c *C) {
	e := newMysqlErr(tmysql.ErrNoDB, "no db error")
	r := isRetryableError(e)
	c.Assert(r, IsFalse)

	e = newMysqlErr(tmysql.ErrUnknown, "i/o timeout")
	r = isRetryableError(e)
	c.Assert(r, IsTrue)

	e = newMysqlErr(tmysql.ErrDBCreateExists, "db is already exists")
	r = isRetryableError(e)
	c.Assert(r, IsFalse)

	ee := errors.New("driver: bad connection")
	r = isRetryableError(ee)
	c.Assert(r, IsTrue)
}
