package kv

import "code.google.com/p/leveldb-go/leveldb/db"
import "testing"
import "os"
import "time"

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
	value := ldbe.Increment("testincrementer")
	if ldbe.GetCounter("testincrementer") != 1 || value != 1 {
		t.Error("Failed to increment counter")
	}
	cleanup("test4.ldb")
}

func TestLeveldbDecrement(t *testing.T) {
	ldbe := NewLevelDBEngine("test5.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	value := ldbe.Decrement("testdecrementer")
	if ldbe.GetCounter("testdecrementer") != -1 || value != -1 {
		t.Error("Failed to increment counter")
	}
	cleanup("test5.ldb")
}

func TestLeveldbEnqueueSet(t *testing.T) {
	ldbe := NewLevelDBEngine("test6.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	for x := 0; x < 10; x++ {
		ldbe.EnqueueSet(string(x), []byte(string(x)))
	}
	time.Sleep(10 * time.Second)
	for x := 0; x < 10; x++ {
		if string(ldbe.Get(string(x))) != string(x) {
			t.Error("RuhRoh Shaggy, didn't get the string we expected")
		}
	}
	cleanup("test6.ldb")
}

func TestLeveldbEnqueueDelete(t *testing.T) {
	ldbe := NewLevelDBEngine("test7.ldb", &db.Options{VerifyChecksums: true}, &db.WriteOptions{Sync: true}, nil)
	for x := 0; x < 10; x++ {
		ldbe.EnqueueSet(string(x), []byte(string(x)))
	}
	time.Sleep(10 * time.Second)
	for x := 0; x < 10; x++ {
		ldbe.EnqueueDelete(string(x))
	}
	time.Sleep(10 * time.Second)
	for x := 0; x < 10; x++ {
		if string(ldbe.Get(string(x))) == string(x) {
			t.Error("Zoinks scooby, that string equality is ghostly")
		}
	}
	cleanup("test7.ldb")
}
