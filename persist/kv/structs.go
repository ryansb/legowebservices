package kv

import (
	tiedot "github.com/HouzuoGuo/tiedot/db"
	"github.com/ryansb/legowebservices/log"
	. "github.com/ryansb/legowebservices/util/m"
)

type Query struct {
	q   []M
	col *tiedot.Col
}

type Path []string

type ResultSet map[uint64]struct{}

// Implemented the KVEngine interface
// This one is just for a local leveldb database
type TiedotEngine struct {
	tiedot *tiedot.DB
}

// Create a new LevelDBEngine with the given file and options
func NewTiedotEngine(directory string, collections []string, dropIfExist bool) *TiedotEngine {
	db, err := tiedot.OpenDB(directory)
	log.FatalIfErr(err, "Failure opening tiedot basedir err:")
	for _, c := range collections {
		if _, ok := db.StrCol[c]; ok {
			log.V(4).Info("Collection %s already exists")
			if dropIfExist {
				log.Info("Dropping collection %s due to dropIfExist option")
				err = db.Drop(c)
				log.FatalIfErr(err, "Failure dropping collection with name:%s err:", c)
				err = db.Create(c, 3) // partition DB for use by up to 3 goroutines at a time
				log.FatalIfErr(err, "Failure creating collection with name:%s err:", c)
			}
		} else {
			log.V(4).Info("Creating collection %s")
			err = db.Create(c, 3) // partition DB for use by up to 3 goroutines at a time
			log.FatalIfErr(err, "Failure creating collection with name:%s err:", c)
		}
	}
	tde := &TiedotEngine{
		tiedot: db,
	}
	return tde
}
