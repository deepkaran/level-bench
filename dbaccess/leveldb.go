package dbaccess

import (
	. "github.com/jmhodges/levigo"
	"log"
	"time"
)

type LevelDB struct {
	name    string
	options *Options
	ro      *ReadOptions
	wo      *WriteOptions
	c       *DB
}

func (ldb *LevelDB) Init(name string) {

	ldb.name = name

	ldb.options = NewOptions()
	ldb.options.SetCreateIfMissing(true)
	ldb.options.SetCompression(NoCompression)

	//set filter policy
	//	filterPolicy := NewBloomFilter(10)
	//	ldb.options.SetFilterPolicy(filterPolicy)

	//	ldb.options.SetCompression(SnappyCompression)

	ldb.wo = NewWriteOptions()
	ldb.ro = NewReadOptions()

	var err error
	ldb.c, err = Open(ldb.name, ldb.options)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}

}

func (ldb *LevelDB) Set(k string, v string) (time.Duration, error) {

	start := time.Now()
	err := ldb.c.Put(ldb.wo, []byte(k), []byte(v))
	elapsed := time.Since(start)
	return elapsed, err

}

func (ldb *LevelDB) Get(k string) (string, time.Duration, error) {

	start := time.Now()
	v, err := ldb.c.Get(ldb.ro, []byte(k))
	elapsed := time.Since(start)
	return string(v), elapsed, err

}

func (ldb *LevelDB) Delete(k string) (time.Duration, error) {

	start := time.Now()
	err := ldb.c.Delete(ldb.wo, []byte(k))
	elapsed := time.Since(start)
	return elapsed, err

}

func (ldb *LevelDB) Close() {
	if ldb.c != nil {
		ldb.c.Close()
		DestroyDatabase(ldb.name, ldb.options)
	}

}
