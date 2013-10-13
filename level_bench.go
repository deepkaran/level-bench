package main

import (
	"log"
	"sync"
) 

var keyList []string
var keyCount int64
var db DBInfo
var fds FileDataSource

type BenchConf struct {
    name string
    workList []Workload
    reInitSetup bool
}

func main() {

	conf := confInit()
    
    for _, c := range conf {
    	if c.reInitSetup == true {
		    reInitSetup()	
    	}
    	var wg sync.WaitGroup
    	log.Printf("Picked up conf %s", c.name)
    	for _, w := range c.workList {
    		wg.Add(1)
			go w.RunWorkload(db, fds, &wg)
    	}
    	wg.Wait()
	}
}

func reInitSetup() {
	db.Close()
	db.Init("bench")
    fds.Init("data")
	keyCount = 0
	keyList = nil
}


func confInit() ([] BenchConf) {

	var conf []BenchConf
/*
//CONF1 CREATE 10M --> READ 10M
        {
                var c BenchConf
                c.name = "CREATE 10M"
                c.workList = append(c.workList, Workload{1, 0, 0, 0, 10000000})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "READ 10M"
                c.workList = append(c.workList, Workload{0, 1, 0, 0, 10000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }

//CONF2 CREATE 5M --> CREATE(5M) AND UPDATE (5M)
        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{1, 0, 0, 0, 5000000})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE 5M AND UPDATE 5M"
                c.workList = append(c.workList, Workload{0.5, 0, 0.5, 0, 10000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }

//CONF1 CREATE 5M --> CREATE(5M) AND READ(5M) -> UPDATE(5M) AND READ(5M)        
        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{1, 0, 0, 0, 5000000})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE 5M READ 5M"
                c.workList = append(c.workList, Workload{0.5, 0.5, 0, 0, 10000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "UPDATE 5M READ 5M"
                c.workList = append(c.workList, Workload{0, 0.5, 0.5, 0, 10000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }
        
//CONF4 CREATE(5M) --> CREATE(5M) AND READ(5M) IN PARALLEL 
//      --> UPDATE(5M) AND READ(5M) IN PARALLEL

        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{1, 0, 0, 0, 5000000})  
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE 5M READ 5M PARALLEL"
                c.workList = append(c.workList, Workload{1, 0, 0, 0, 5000000})
                c.workList = append(c.workList, Workload{0, 1, 0, 0, 5000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }
        
        
        {
                var c BenchConf
                c.name = "UPDATE 5M READ 5M PARALLEL"
                c.workList = append(c.workList, Workload{0, 0, 1, 0, 5000000})
                c.workList = append(c.workList, Workload{0, 1, 0, 0, 5000000})
                c.reInitSetup = false
                conf = append(conf, c)
        }
*/
//CONF5 CREATE(5M) --> CREATE(5M) AND READ (5M) 4 THREADS

        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{"CREATE5M", 1, 0, 0, 0, 5000000, false})  
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE(5M) AND READ (5M) 4 THREADS"
                c.workList = append(c.workList, Workload{"CREATE5M", 1, 0, 0, 0, 5000000, true})
                c.workList = append(c.workList, Workload{"UPDATE5M", 0, 0, 1, 0, 25000000, true})
                c.workList = append(c.workList, Workload{"READ15M", 0, 1, 0, 0, 25000000, true})
                c.workList = append(c.workList, Workload{"READ25M", 0, 1, 0, 0, 25000000, true})
                c.workList = append(c.workList, Workload{"READ35M", 0, 1, 0, 0, 25000000, true})
                c.workList = append(c.workList, Workload{"READ45M", 0, 1, 0, 0, 25000000, true})
                c.reInitSetup = false
                conf = append(conf, c)
        }
        
        return conf

}

