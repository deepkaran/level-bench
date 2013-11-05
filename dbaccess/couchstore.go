package dbaccess

import (
	. "github.com/deepkaran/gocouchstore"
	"log"
	"os"
	"strconv"
	"time"
)

type CouchStore struct {
	name              string
	compactNum        int
	WaitForCompaction bool
	compactName       string
}

func (cs *CouchStore) Init(name string) {

	cs.name = name
	cs.compactName = name
	cs.compactNum = 1
	cs.WaitForCompaction = false

}

func (cs *CouchStore) Set(k string, v string) (time.Duration, error) {

	start := time.Now()
	c, err := OpenRW(cs.name)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	err = c.Put([]byte(k), []byte(""), []byte(v))
	c.Commit()
	c.Close()
	elapsed := time.Since(start)
	return elapsed, err
}

func (cs *CouchStore) Get(k string) (string, time.Duration, error) {

	start := time.Now()
	c, err := OpenRO(cs.name)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	v, err1 := c.Get([]byte(k))
	c.Close()
	elapsed := time.Since(start)
	return string(v), elapsed, err1

}

func (cs *CouchStore) Delete(k string) (time.Duration, error) {

	start := time.Now()
	c, err := OpenRW(cs.name)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	err = c.Delete([]byte(k))
	c.Commit()
	c.Close()
	elapsed := time.Since(start)
	return elapsed, err

}

func (cs *CouchStore) Compact() {

	log.Println("Reached Compaction")
	compactName := cs.compactName + strconv.Itoa(cs.compactNum)

	start := time.Now()
	c, err := OpenRW(cs.name)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	err = c.Compact(compactName)
	c.Close()
	elapsed := time.Since(start)
	log.Printf("Time spent in compaction %d %v", cs.compactNum, elapsed)
	if err != nil {
		log.Printf("DB Error in Compact : %v", err)
	}
	cs.compactNum++
	cs.WaitForCompaction = true
	time.Sleep(time.Microsecond * 10) //wait for readers to stop
	os.Remove(cs.name)
	cs.name = compactName
	cs.WaitForCompaction = false

}

func (cs *CouchStore) Close() {

}
