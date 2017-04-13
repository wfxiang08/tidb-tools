package route

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRouteSuite{})

type testRouteSuite struct{}

func (t *testRouteSuite) TestRoute(c *C) {
	r := NewTrieRouter()
	t.testInsert(c, r)
	t.testMatch(c, r)
}

func (t *testRouteSuite) testInsert(c *C, r Router) {
	var err error
	cases := map[string]string{
		"?bc":  "abc1",
		"a?c":  "abc2",
		"a??":  "abc3",
		"a?*":  "abc4",
		"ab*":  "abc5",
		"xyz":  "abc6",
		"xyy*": "abc7",
	}
	for pattern, target := range cases {
		err = r.Insert(pattern, target)
		c.Assert(err, IsNil)
	}
	rules := r.AllRules()
	c.Assert(rules, DeepEquals, cases)
	// test error
	err = r.Insert("ab**", "error")
	c.Assert(err, NotNil)
	err = r.Insert("ab*", "error")
	c.Assert(err, NotNil)
	// check all rules again
	rules = r.AllRules()
	c.Assert(rules, DeepEquals, cases)
}

func (t *testRouteSuite) testMatch(c *C, r Router) {
	cases := map[string]string{
		"dbc":        "abc1",
		"adc":        "abc4",
		"add":        "abc4",
		"abd":        "abc4",
		"ab":         "abc4",
		"xyz":        "abc6",
		"xyy":        "abc7",
		"xyyyyyyyyy": "abc7",
		"xxxx":       "",
	}
	for pattern, target := range cases {
		c.Assert(r.Match(pattern), Equals, target)
	}

	// test cache
	trie, ok := r.(*trieRouter)
	c.Assert(ok, IsTrue)
	c.Assert(trie.cache, DeepEquals, cases)
}
