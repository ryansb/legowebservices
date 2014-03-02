package kv

import (
	"errors"
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	"strings"
)

var ErrNotFound = errors.New("legowebservices/persist/kv: Error not found")

func (t *TiedotEngine) AddIndex(collection string, path []string) error {
	c := t.tiedot.Use(collection)
	if _, ok := c.SecIndexes[strings.Join(path, tiedot.INDEX_PATH_SEP)]; ok {
		log.Info("Index on path:%v already exists for collection:%s", path, collection)
		return nil
	}
	log.V(3).Info("Adding index on path:%v to collection:%s", path, collection)
	return c.Index(path)
}

func (t *TiedotEngine) Collection(collection string) *tiedot.Col {
	return t.tiedot.Use(collection)
}

func (t *TiedotEngine) DB() *tiedot.DB {
	return t.tiedot
}

func (t *TiedotEngine) Query(collectionName string) *Query {
	return &Query{col: t.tiedot.Use(collectionName)}
}

func (t *TiedotEngine) All(collectionName string) (ResultSet, error) {
	r := make(map[uint64]struct{})
	if err := tiedot.EvalQuery("all", t.tiedot.Use(collectionName), &r); err != nil {
		log.Error("Error executing TiedotEngine.All() err=%s", err.Error())
		return nil, err
	}
	return r, nil
}
