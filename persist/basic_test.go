package persist

import (
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/ryansb/legowebservices/persist/kv"
	. "github.com/ryansb/legowebservices/util/m"
	. "launchpad.net/gocheck"
	"testing"
)

func init() {
	tdlog.VerboseLog = false
}

func Test(t *testing.T) { TestingT(t) }

type TS struct{}

var _ = Suite(&TS{})

func (s *TS) TestNewCollection(c *C) {
	engine := kv.NewTiedotEngine("./tmp", []string{"fake", "anotherfake"}, kv.KeepIfExist)

	err := engine.Insert("fake", M{"name": "Bob", "age": 42})
	c.Check(err, Equals, nil)
}
