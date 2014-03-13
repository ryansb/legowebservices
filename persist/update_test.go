package persist

import (
	"github.com/ryansb/legowebservices/persist/kv"
	. "launchpad.net/gocheck"
)

func (s *TS) TestUpdateValue(c *C) {
	engine := getEngine()
	defer engine.DB().Close()
	engine.AddIndex("fake", kv.Path{"Name"})

	id, err := engine.Insert("fake", person{Name: "Bob", Age: 42})
	c.Assert(err, Equals, nil)

	err = engine.Update("fake", id, person{Name: "Joe"})
	c.Check(err, Equals, nil)

	res, err := engine.Query("fake").Equals(kv.Path{"Name"}, "Joe").All()
	c.Assert(len(res), Equals, 1)
}
