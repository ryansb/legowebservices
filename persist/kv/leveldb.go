package kv

import (
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/binary"
	"github.com/ryansb/legowebservices/log"
	"sync"
	"time"
)

// Implemented the KVEngine interface
// This one is just for a local leveldb database
type LevelDBEngine struct {
	levelDB         *leveldb.DB
	writeOpts       *db.WriteOptions
	readOpts        *db.ReadOptions
	batchSetChan    chan map[string][]byte
	batchDeleteChan chan []byte
	countMutex      *sync.Mutex
}

// Create a new LevelDBEngine with the given file and options
func NewLevelDBEngine(file string, options *db.Options, woptions *db.WriteOptions, roptions *db.ReadOptions) *LevelDBEngine {
	levelDB, err := leveldb.Open(file, options)
	log.FatalIfErr(err, "Failure opening leveldb file err:")
	ldbe := &LevelDBEngine{
		levelDB:         levelDB,
		writeOpts:       woptions,
		readOpts:        roptions,
		batchSetChan:    make(chan map[string][]byte, 10),
		batchDeleteChan: make(chan []byte, 10),
		countMutex:      new(sync.Mutex),
	}
	go ldbe.BatchSync()
	return ldbe
}

// Set a key in leveldb returns true if the key has been set
func (ldbe *LevelDBEngine) Set(key string, value []byte) bool {
	err := ldbe.levelDB.Set([]byte(key), value, ldbe.writeOpts)
	if err != nil {
		log.Warningf("Failed to set '%s': %s", key, err)
		return false
	}
	return true
}

// Get a key from leveldb
func (ldbe *LevelDBEngine) Get(key string) []byte {
	value, err := ldbe.levelDB.Get([]byte(key), ldbe.readOpts)
	if err != nil {
		log.Infof("Failed to get '%s': %s", key, err)
	}
	return value
}

// Delete a key from leveldb
// Returns a bool is the key is successfully deleted
func (ldbe *LevelDBEngine) Delete(key string) bool {
	err := ldbe.levelDB.Delete([]byte(key), ldbe.writeOpts)
	if err != nil {
		log.Warningf("Failed to delete '%s': %s", key, err)
		return false
	}
	return true
}

func (ldbe *LevelDBEngine) Find(key string) []byte {
	// uhhhh, wat
	panic("unimplemented")
}

// Stage a delete operation and put it into the batch channel
// It will be added to a batch and eventually synced to leveldb
func (ldbe *LevelDBEngine) EnqueueDelete(key string) {
	log.V(5).Info("Enqueued delete for key:" + key)
	ldbe.batchDeleteChan <- []byte(key)
}

// Stage a set operation and put it into the batch channel
// It will be added to a batch and eventually synced to leveldb
func (ldbe *LevelDBEngine) EnqueueSet(key string, value []byte) {
	log.V(5).Info("Enqueued write for key:" + key)
	ldbe.batchSetChan <- map[string][]byte{key: value}
}

// Run as a go routine in order to aggregate BatchSet and BatchDelete operations
// Operations come in off the channel and are synced when there are 10 operations
// in the batch, or after 10 seconds
func (ldbe *LevelDBEngine) BatchSync() {
	batch := leveldb.Batch{}
	numOps := 0

	// Our Flush function which writes out batch out to leveldb
	flush := func() {
		err := ldbe.levelDB.Apply(batch, ldbe.writeOpts)
		if err != nil {
			// If we fail we don't reset our batch or op counter
			// that way we retry if we fail
			log.Errorf("Failed to sync batch data: %s", err)
			return
		}
		log.V(4).Info("Flushed batch of %d writes/deletes", numOps)
		batch = leveldb.Batch{}
		numOps = 0
	}
	for {
		select {
		// Grab a BatchDelete Operation from the channel
		case delKey := <-ldbe.batchDeleteChan:
			batch.Delete(delKey)
			numOps += 1
			// Grab a set operation from the channe;
		case setMap := <-ldbe.batchSetChan:
			for key, val := range setMap {
				batch.Set([]byte(key), val)
			}
			numOps += 1
			// Time out after 10 seconds and flush our batch
		case <-time.After(time.Second * 10):
			if numOps > 0 {
				log.V(5).Info("Flushing %d writes after timeout", numOps)
				flush()
			}
		}
		// Flush if we have >= 10 operations in our batch
		if numOps >= 10 {
			flush()
		}
	}
}

func (ldbe *LevelDBEngine) GetCounter(key string) int64 {
	count, numRead := binary.Varint(ldbe.Get(key))
	if numRead <= 0 {
		count = 0
	}
	return count
}

// "Atomic" increment of a counter key in leveldb
// It is surrounded by a mutex so only one routine can incr/decr at a time
func (ldbe *LevelDBEngine) atomicAdd(key string, addBy int64) int64 {
	ldbe.countMutex.Lock()
	log.V(4).Info("Got atomicAdd lock for key:%s adding:%d", key, addBy)

	defer ldbe.countMutex.Unlock()
	defer log.V(4).Info("Returned atomicAdd lock for key:%s", key)

	count := ldbe.GetCounter(key)
	count += addBy
	countBytes := make([]byte, 8)
	numWritten := binary.PutVarint(countBytes, count)
	if numWritten <= 0 {
		log.Errorf("Failed to encode counter '%s' to binary", count)
	} else {
		if !ldbe.Set(key, countBytes) {
		}
	}
	return count
}
func (ldbe *LevelDBEngine) Increment(key string) int64 {
	return ldbe.atomicAdd(key, 1)
}

// "Atomic" decrement of a counter key in leveldb
// It is surrounded by a mutex so only one routine can incr/decr at a time
func (ldbe *LevelDBEngine) Decrement(key string) int64 {
	return ldbe.atomicAdd(key, -1)
}
