package kv

import (
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	"github.com/ryansb/legowebservices/util/m"
)

type DropPreference uint8

const (
	DropIfExist DropPreference = iota
	KeepIfExist
)

type LockPreference uint8

const (
	MustLock LockPreference = iota
	NoLock
)

type Query struct {
	q        []m.M
	col      *tiedot.Col
	ReadLock LockPreference
}

type Path []string
type ResultSet map[uint64]interface{}
type RawResultSet map[uint64]struct{}

// Implemented the KVEngine interface
type TiedotEngine struct {
	tiedot *tiedot.DB
}

func (tde *TiedotEngine) Close() {
	tde.tiedot.Close()
}

// Create a new LevelDBEngine with the given file and options
func NewTiedotEngine(directory string, collections []CollectionParams, dropPref DropPreference) *TiedotEngine {
	db, err := tiedot.OpenDB(directory)
	log.FatalIfErr(err, "Failure opening tiedot basedir err:")
	for _, c := range collections {
		if c.Partitions < 1 {
			c.Partitions = 1
		}
		if _, ok := db.StrCol[c.Name]; ok {
			log.V(4).Info("Collection %s already exists")
			if dropPref == DropIfExist {
				log.Info("Dropping collection %s due to dropIfExist option")
				err = db.Drop(c.Name)
				log.FatalIfErr(err, "Failure dropping collection with name:%s err:", c)
				err = db.Create(c.Name, c.Partitions) // partition DB for use by up to 1 goroutines at a time
				log.FatalIfErr(err, "Failure creating collection with name:%s err:", c)
			}
		} else {
			log.V(4).Info("Creating collection %s")
			err = db.Create(c.Name, c.Partitions) // partition DB for use by up to 1 goroutines at a time
			log.FatalIfErr(err, "Failure creating collection with name:%s err:", c)
		}
	}
	tde := &TiedotEngine{
		tiedot: db,
	}

	for _, c := range collections {
		for _, index := range c.Indices {
			tde.AddIndex(c.Name, index)
		}
	}

	return tde
}
