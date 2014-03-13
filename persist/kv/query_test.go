package kv

import (
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type TS struct{}

var _ = Suite(&TS{})

func (s *TS) TestNewCollection(c *C) {
	q := new(Query)
	q.Equals(Path{"contact", "name"}, "bob")

	c.Check(len(q.q), Equals, 1)
	c.Check(q.q[0]["eq"], Equals, "bob")
	c.Check(len(q.q[0]["in"].(Path)), Equals, 2)
}
