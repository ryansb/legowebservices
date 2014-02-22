package kv

import "code.google.com/p/leveldb-go/leveldb/db"
import "testing"
import "os"

func cleanup(dbname string) {
	os.RemoveAll(dbname)
}

func TestLeveldbOpen(t *testing.T) {
	ldbe := NewLevelDBEngine("test1.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	if ldbe == nil {
		t.Error("Failed to create ldbe")
	}
	cleanup("test1.ldb")

}

func TestLeveldbSet(t *testing.T) {
	ldbe := NewLevelDBEngine("test2.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	if !ldbe.Set("testkey", []byte("test")) {
		t.Error("failed to set key in ldbe")
	}
	cleanup("test2.ldb")
}

func TestLeveldbGet(t *testing.T) {
	ldbe := NewLevelDBEngine("test3.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	if !ldbe.Set("othertestkey", []byte("test")) {
		t.Error("failed to set key in ldbe for get test")
	}
	val := ldbe.Get("othertestkey")
	if string(val) != "test" {
		t.Error("Failed to get correct key back")
	}
	cleanup("test3.ldb")
}

func TestLeveldbIncrement(t *testing.T) {
	ldbe := NewLevelDBEngine("test4.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	ldbe.Increment("testincrementer")
	if ldbe.GetCounter("testincrementer") != 1 {
		t.Error("Failed to increment counter")
	}
	cleanup("test4.ldb")
}

func TestLeveldbDecrement(t *testing.T) {
	ldbe := NewLevelDBEngine("test5.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	ldbe.Decrement("testdecrementer")
	if ldbe.GetCounter("testdecrementer") != -1 {
		t.Error("Failed to increment counter")
	}
	cleanup("test5.ldb")
}
