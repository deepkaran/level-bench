package dbaccess

import (
	. "github.com/couchbaselabs/goforestdb"
	"log"
	"time"
)

type ForestDB struct {
	name string
	c    [2]*Database //ForestDB needs separate handle for read and write
}

func (fdb *ForestDB) Init(name string) {

	fdb.name = name

	var err error

	config := DefaultConfig()
	config.SetDurabilityOpt(DRB_ASYNC)
	config.SetCompactionMode(COMPACT_AUTO)

	for i := 0; i < 2; i++ {
		fdb.c[i], err = Open(fdb.name, config)
		if err != nil {
			log.Fatalf("Open failed: %v", err)
		}
	}

}

func (fdb *ForestDB) Set(k string, v string) (time.Duration, error) {

	start := time.Now()
	err := fdb.c[0].SetKV([]byte(k), []byte(v))
	elapsed := time.Since(start)
	fdb.c[0].Commit(COMMIT_NORMAL)
	return elapsed, err
}

func (fdb *ForestDB) Get(k string) (string, time.Duration, error) {

	start := time.Now()
	v, err := fdb.c[1].GetKV([]byte(k))
	elapsed := time.Since(start)
	return string(v), elapsed, err

}

func (fdb *ForestDB) Delete(k string) (time.Duration, error) {

	start := time.Now()
	err := fdb.c[0].DeleteKV([]byte(k))
	elapsed := time.Since(start)
	fdb.c[0].Commit(COMMIT_NORMAL)
	return elapsed, err

}

func (fdb *ForestDB) Close() {

	if fdb.c[0] != nil {
		fdb.c[0].Close()
		fdb.c[1].Close()
		Shutdown()
	}
}
