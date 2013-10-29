package dbaccess

import (
	"fmt"
	"github.com/couchbaselabs/indexing/btree"
	"os"
	"time"
)

type CBtreeDB struct {
	name string
	bt   *btree.BTree
}

var conf = btree.Config{
	Idxfile: "./cbtree/cbtreeIndexfile.dat",
	Kvfile:  "./cbtree/cbtreeKvfile.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  4 * 1024,
	},
	Maxlevel:      6,
	RebalanceThrs: 25,
	AppendRatio:   0.7,
	DrainRate:     200,
	MaxLeafCache:  1000,
	Sync:          false,
	Nocache:       false,
}

func (dbi *CBtreeDB) Init(name string) {
	fmt.Println("Initialize...")
	os.MkdirAll("./cbtree", 0770)
	dbi.name = name
	//os.Remove(conf.Idxfile)
	//os.Remove(conf.Kvfile)
	dbi.bt = btree.NewBTree(btree.NewStore(conf))
}

func (dbi *CBtreeDB) Set(k string, v string) (time.Duration, error) {
	start := time.Now()
	dbi.bt.Insert(&btree.TestKey{k, 0}, &btree.TestValue{v})
	elapsed := time.Since(start)
	return elapsed, nil

}

func (dbi *CBtreeDB) Get(k string) (string, time.Duration, error) {
	start := time.Now()
	ch := dbi.bt.Lookup(&btree.TestKey{k, 0})
	vals := make([]string, 0)
	val := <-ch
	for val != nil {
		vals = append(vals, string(val))
		val = <-ch
	}
	elapsed := time.Since(start)
	if len(vals) > 0 {
		return string(vals[0]), elapsed, nil
	} else {
		return "", elapsed, nil
	}

}

func (dbi *CBtreeDB) Delete(k string) (time.Duration, error) {
	start := time.Now()
	dbi.bt.Remove(&btree.TestKey{k, 0})
	elapsed := time.Since(start)
	return elapsed, nil
}

func (dbi *CBtreeDB) Close() {
	if dbi.bt != nil {
		dbi.bt.Close()
		//os.Remove(conf.Idxfile)
		//os.Remove(conf.Kvfile)
		os.RemoveAll("./cbtree")
	}
}
