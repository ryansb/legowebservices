package persist

import (
	"encoding/json"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/ryansb/legowebservices/persist/kv"
	. "github.com/ryansb/legowebservices/util/m"
	. "launchpad.net/gocheck"
	"os"
	"testing"
)

func init() {
	tdlog.VerboseLog = false
}

func Test(t *testing.T) { TestingT(t) }

type TS struct{}

var _ = Suite(&TS{})

func (s *TS) TearDownTest(c *C) {
	c.Check(os.RemoveAll("./tmp"), Equals, nil)
}

func getEngine() *kv.TiedotEngine {
	return kv.NewTiedotEngine("./tmp", []string{"fake"}, kv.DropIfExist)
}

func (s *TS) TestNewCollection(c *C) {
	engine := getEngine()
	defer engine.DB().Close()

	err := engine.Insert("fake", M{"name": "Bob", "age": 42})
	c.Check(err, Equals, nil)
}

type person struct {
	Name string
	Age  int
}

func (p person) ToM() M {
	return M{"Name": p.Name, "Age": p.Age}
}

func (s *TS) TestSimpleGet(c *C) {
	engine := getEngine()
	defer engine.DB().Close()
	engine.AddIndex("fake", kv.Path{"Name"})

	err := engine.Insert("fake", person{Name: "Bob", Age: 42})
	c.Assert(err, Equals, nil)

	res, err := engine.Query("fake").Equals(kv.Path{"Name"}, "Bob").All()
	c.Check(err, Equals, nil)

	c.Assert(len(res), Equals, 1)
	for _, v := range res {
		m, err := json.Marshal(v)
		c.Assert(err, Equals, nil)
		bob := person{}
		err = json.Unmarshal(m, &bob)
		c.Assert(err, Equals, nil)
		c.Check(bob.Name, Equals, "Bob")
		c.Check(bob.Age, Equals, 42)
	}
}
