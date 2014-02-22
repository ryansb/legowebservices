package kv

import (
	"code.google.com/p/leveldb-go/leveldb"
	"code.google.com/p/leveldb-go/leveldb/db"
	"encoding/binary"
	"log"
	"sync"
	"time"
)

// Implemented the KVEngine interface
// This one is just for a local leveldb database
type LevelDBEngine struct {
	LevelDB         *leveldb.DB
	WriteOpts       *db.WriteOptions
	ReadOpts        *db.ReadOptions
	BatchSetChan    chan map[string][]byte
	BatchDeleteChan chan []byte
	CountMutex      *sync.Mutex
}

// Create a new LevelDBEngine with the given file and options
func NewLevelDBEngine(file string, options *db.Options, woptions *db.WriteOptions, roptions *db.ReadOptions) *LevelDBEngine {
	ldbe := new(LevelDBEngine)
	var err error
	ldbe.LevelDB, err = leveldb.Open(file, options)
	if err != nil {
		panic(err)
	}
	ldbe.WriteOpts = woptions
	ldbe.ReadOpts = roptions
	ldbe.BatchSetChan = make(chan map[string][]byte, 10)
	ldbe.BatchDeleteChan = make(chan []byte, 10)
	ldbe.CountMutex = new(sync.Mutex)
	go ldbe.BatchSync()
	return ldbe
}

// Set a key in leveldb returns true if the key has been set
func (ldbe *LevelDBEngine) Set(key string, value []byte) bool {
	err := ldbe.LevelDB.Set([]byte(key), value, ldbe.WriteOpts)
	if err != nil {
		log.Print("Failed to set '", key, "':", err)
		return false
	}
	return true
}

// Get a key from leveldb
func (ldbe *LevelDBEngine) Get(key string) []byte {
	value, err := ldbe.LevelDB.Get([]byte(key), ldbe.ReadOpts)
	if err != nil {
		log.Print("Failed to get '", key, "': ", err)
	}
	return value
}

// Delete a key from leveldb
// Returns a bool is the key is successfully deleted
func (ldbe *LevelDBEngine) Delete(key string) bool {
	err := ldbe.LevelDB.Delete([]byte(key), ldbe.WriteOpts)
	if err != nil {
		log.Print("Failed to delete '", key, "': ", err)
		return false
	}
	return true
}

func (ldbe *LevelDBEngine) Find(key string) bool {
	// uhhhh, wat
	panic("unimplemented")
}

// Stage a delete operation and put it into the batch channel
// It will be added to a batch and eventually synced to leveldb
func (ldbe *LevelDBEngine) BatchDelete(key string) {
	ldbe.BatchDeleteChan <- []byte(key)
}

// Stage a set operation and put it into the batch channel
// It will be added to a batch and eventually synced to leveldb
func (ldbe *LevelDBEngine) BatchSet(key string, value []byte) {
	ldbe.BatchSetChan <- map[string][]byte{key: value}
}

// Run as a go routine in order to aggregate BatchSet and BatchDelete operations
// Operations come in off the channel and are synced when there are 10 operations
// in the batch, or after 10 seconds
func (ldbe *LevelDBEngine) BatchSync() {
	batch := leveldb.Batch{}
	numOps := 0

	// Our Flush function which writes out batch out to leveldb
	flush := func() {
		err := ldbe.LevelDB.Apply(batch, ldbe.WriteOpts)
		if err != nil {
			// If we fail we don't reset our batch or op counter
			// that way we retry if we fail
			log.Print("Failed to sync batch data:", err)
			return
		}
		batch = leveldb.Batch{}
		numOps = 0
	}
	for {
		select {
		// Grab a BatchDelete Operation from the channel
		case delKey := <-ldbe.BatchDeleteChan:
			batch.Delete(delKey)
			numOps += 1
			// Grab a set operation from the channe;
		case setMap := <-ldbe.BatchSetChan:
			for key, val := range setMap {
				batch.Set([]byte(key), val)
			}
			numOps += 1
			// Time out after 10 seconds and flush our batch
		case <-time.Tick(time.Second * 10):
			flush()
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
		log.Print("Failed to decode counter value")
		count = 0
	}
	return count
}

// "Atomic" increment of a counter key in leveldb
// It is surrounded by a mutex so only one routine can incr/decr at a time
func (ldbe *LevelDBEngine) atomicAdd(key string, addBy int64) {
	ldbe.CountMutex.Lock()
	count := ldbe.GetCounter(key)
	count += addBy
	countBytes := make([]byte, 8)
	numWritten := binary.PutVarint(countBytes, count)
	if numWritten <= 0 {
		log.Print("Failed to encode counter to binary")
	} else {
		if !ldbe.Set(key, countBytes) {
			log.Print("Failed to atomic add key")
		}
	}
	ldbe.CountMutex.Unlock()
}
func (ldbe *LevelDBEngine) Increment(key string) {
	ldbe.atomicAdd(key, 1)
}

// "Atomic" decrement of a counter key in leveldb
// It is surrounded by a mutex so only one routine can incr/decr at a time
func (ldbe *LevelDBEngine) Decrement(key string) {
	ldbe.atomicAdd(key, -1)
}
