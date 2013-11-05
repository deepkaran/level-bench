package dbaccess

import (
	. "github.com/deepkaran/goforestdb"
	"log"
	"strconv"
	"time"
)

type ForestDB struct {
	name              string
	c                 [2]*Conn //ForestDB needs separate handle for read and write
	compactNum        int
	compactName       string
	WaitForCompaction bool
}

func (fdb *ForestDB) Init(name string) {

	fdb.name = name

	var err error

	for i := 0; i < 2; i++ {
		fdb.c[i], err = Open(fdb.name)
		if err != nil {
			log.Fatalf("Open failed: %v", err)
		}
	}

	fdb.compactNum = 1
	fdb.WaitForCompaction = false

}

func (fdb *ForestDB) Set(k string, v string) (time.Duration, error) {

	start := time.Now()
	err := fdb.c[0].Put([]byte(k), []byte(""), []byte(v))
	fdb.c[0].Commit()
	elapsed := time.Since(start)
	return elapsed, err
}

func (fdb *ForestDB) Get(k string) (string, time.Duration, error) {

	start := time.Now()
	v, err := fdb.c[1].Get([]byte(k))
	elapsed := time.Since(start)
	return string(v), elapsed, err

}

func (fdb *ForestDB) Delete(k string) (time.Duration, error) {

	start := time.Now()
	err := fdb.c[0].Delete([]byte(k))
	fdb.c[0].Commit()
	elapsed := time.Since(start)
	return elapsed, err

}

func (fdb *ForestDB) Compact() {

	log.Println("Reached Compaction")
	fdb.compactName = fdb.name + strconv.Itoa(fdb.compactNum)
	start := time.Now()
	err := fdb.c[0].Compact(fdb.compactName)
	elapsed := time.Since(start)
	log.Printf("Time spent in compaction %d %v", fdb.compactNum, elapsed)
	if err != nil {
		log.Printf("DB Error in Compact : %v", err)
	}
	fdb.compactNum++
	fdb.WaitForCompaction = true
	time.Sleep(time.Microsecond * 10) //wait for readers to stop
	fdb.reopenConn()
	fdb.WaitForCompaction = false

}

func (fdb *ForestDB) Close() {

	if fdb.c[0] != nil {
		fdb.c[0].Close()
		fdb.c[1].Close()
		Shutdown()
	}
}

func (fdb *ForestDB) reopenConn() {

	var err error
	fdb.c[0].Close()
	fdb.c[1].Close()
	for i := 0; i < 2; i++ {
		fdb.c[i], err = Open(fdb.compactName)
		if err != nil {
			log.Fatalf("Open failed: %v", err)
		}
	}

}
