package main

import (
	"log"
	"math/rand"
	"time"
	"sync"
)

type Workload struct {
    ratioCreate float64
    ratioRead float64
    ratioUpdate float64
    ratioDelete float64
    totalOps int64
}

const (
	CREATE = iota
	READ = iota
	UPDATE = iota
	DELETE = iota
)

func runWorkload(db DBInfo, w Workload, f FileDataSource, wg *sync.WaitGroup) {

	defer wg.Done()

    var timeCreate []time.Duration
    var timeRead   []time.Duration
    var timeUpdate []time.Duration
    var timeDelete []time.Duration

    var opList []int
    
    for c := 0.1; c <= w.ratioCreate; {
        opList = append(opList, CREATE)
        c += 0.1
    }

    for c := 0.1; c <= w.ratioRead; {
        opList = append(opList, READ)
        c += 0.1
    }
    
    for c := 0.1; c <= w.ratioUpdate; {
        opList = append(opList, UPDATE)
        c += 0.1
    }

    for c := 0.1; c <= w.ratioDelete; {
        opList = append(opList, DELETE)
        c += 0.1
    }

	var i int64
    for i = 0; i < w.totalOps; i++ {
    
        switch(opList[i%10]) {
        
        case CREATE:
			k, v := f.Next()
            elapsed, err := db.Set(k, v)
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        timeCreate = append(timeCreate, elapsed)
		    keyList = append(keyList, k)
		    keyCount++
        
        case READ:
        	i := rand.Int63n(keyCount)
            _, elapsed, err := db.Get(keyList[i])
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        timeRead = append(timeRead, elapsed)
        
        case UPDATE:
        	i := rand.Int63n(keyCount)
        	k := keyList[i]
            v, elapsed, err := db.Get(k)
	        if err != nil {
	            log.Fatalf("DB Error in Get : %v", err)
	        }
            elapsed, err = db.Set(k, v)
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        timeUpdate = append(timeUpdate, elapsed)
        
        case DELETE:
        	i := rand.Int63n(keyCount)
            elapsed, err := db.Delete(keyList[i])
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        timeDelete = append(timeDelete, elapsed)
	        //TODO
	        //delete from keyList as well and keyCount--
        }
    }
    
    
    log.Printf("Statistics for Workload %v", w)
    
    genReport(timeCreate, "Create")
    genReport(timeRead, "Read")
    genReport(timeUpdate, "Update")
    genReport(timeDelete, "Delete")

}

func genReport(timeInfo []time.Duration, opType string) {

	if len(timeInfo) == 0 {
		return
	}
	
    var sum float64 = 0
    var count int64 = 0
	for _, x := range timeInfo {
		sum += x.Seconds()
		count += 1
	}
	
	log.Printf("Total Ops for %s : %d", opType, count)
	log.Printf("Total time taken : %f seconds", sum)
	log.Printf("Ops per second : %f", float64 (count) / sum)
}


