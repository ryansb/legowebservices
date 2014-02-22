package kv

// Interface for key value engines
type KVEngine interface {
	Set(key string, value []byte) bool
	EnqueueSet(key string, value []byte)
	Get(key string) []byte
	Delete(key string) bool
	EnqueueDelete(key string)
	Find(key string) []byte
	GetCounter(key string) int64
	Increment(key string) int64
	Decrement(key string) int64
}
