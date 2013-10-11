package main

import (
	"log"
	. "github.com/jmhodges/levigo"
	"time"
) 


type DBInfo struct {
	name string
	options *Options
	ro *ReadOptions
	wo *WriteOptions
	db *DB
}


func (dbi *DBInfo) Init(name string) {


	dbi.name = name
 
	dbi.options = NewOptions()
	dbi.options.SetCreateIfMissing(true)
	dbi.options.SetCompression(NoCompression)
	
//  set filter policy
//	filterPolicy := NewBloomFilter(10)
//	options.SetFilterPolicy(filterPolicy)

//	options.SetCompression(SnappyCompression)

	dbi.wo = NewWriteOptions()
	dbi.ro = NewReadOptions()
 
 	var err error
	dbi.db, err = Open(dbi.name, dbi.options)
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}

}

func (dbi *DBInfo) Set(k string, v string) (time.Duration, error) {

    start := time.Now()
	err := dbi.db.Put(dbi.wo, []byte(k), []byte(v))
    elapsed := time.Since(start)
    return elapsed, err

}

func (dbi *DBInfo) Get(k string) (string, time.Duration, error) {

    start := time.Now()
	v, err := dbi.db.Get(dbi.ro, []byte(k))
    elapsed := time.Since(start)
    return string(v), elapsed, err
    
}

func (dbi *DBInfo) Delete(k string) (time.Duration, error) {

    start := time.Now()
	err := dbi.db.Delete(dbi.wo, []byte(k))
    elapsed := time.Since(start)
    return elapsed, err

}

func (dbi *DBInfo) Close() {
	if dbi.db != nil {
	    dbi.db.Close()
    	DestroyDatabase(dbi.name, dbi.options)
	}
}