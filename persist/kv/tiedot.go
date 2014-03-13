package kv

import (
	"errors"
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	"strings"
)

var ErrNotFound = errors.New("legowebservices/persist/kv: Error not found")
var ErrReadPreference = errors.New("legowebservices/persist/kv: Readpreference not set")

func (t *TiedotEngine) AddIndex(collection string, path Path) {
	c := t.tiedot.Use(collection)
	tdPath := strings.Join(path, tiedot.INDEX_PATH_SEP)
	if _, ok := c.SecIndexes[tdPath]; ok {
		log.Infof("Index on path:%v already exists for collection:%s", tdPath, collection)
		return
	}
	log.V(3).Infof("Adding index on path:%v to collection:%s", tdPath, collection)
	err := c.Index(path)
	log.FatalIfErr(err, "Failure creating index on collection:"+collection)
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

func (t *TiedotEngine) Insert(collectionName string, item Insertable) (uint64, error) {
	if len(item.ToM()) == 0 {
		log.Warningf("Failure: No data in item=%v", item.ToM())
		return 0, nil
	} else {
		log.V(3).Infof("Insertion into collection=%s item=%v",
			collectionName, item.ToM())
	}
	id, err := t.tiedot.Use(collectionName).Insert(item.ToM())
	if err != nil {
		log.Errorf("Failure inserting item=%v err=%s", item.ToM(), err.Error())
		return 0, err
	}
	log.V(6).Infof("Added item with ID=%d, item=%v", id, item.ToM())
	return id, nil
}

func (t *TiedotEngine) Update(collectionName string, id uint64, item Insertable) error {
	log.V(3).Infof("Updating with data: %v", item.ToM())
	if err := t.tiedot.Use(collectionName).Update(id, item.ToM()); err != nil {
		log.Errorf("Failure updating item=%s err=%s", item.ToM().JSON(), err.Error())
		return err
	} else {
		return nil
	}
}

func (t *TiedotEngine) All(collectionName string) (map[uint64]struct{}, error) {
	r := make(map[uint64]struct{})
	if err := tiedot.EvalQuery("all", t.tiedot.Use(collectionName), &r); err != nil {
		log.Error("Error executing TiedotEngine.All() err=%s", err.Error())
		return nil, err
	}
	return r, nil
}
