package main

import (
	"log"
	"sync"
	"time"
) 

var keyList []string
var keyCount int64

var deleteList = struct{
    sync.RWMutex
    m map[int64]bool
}{m: make(map[int64]bool)}

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
    	if c.reInitSetup {
		    reInitSetup()	
    	}
    	var wg sync.WaitGroup
    	log.Printf("Picked up conf %s", c.name)
    	for i := range c.workList {
    		wg.Add(1)
			go c.workList[i].RunWorkload(db, fds, &wg)
			time.Sleep(1)
    	}
		wg.Wait()
		
    	for i := range c.workList  {
    		if c.workList[i].reportStats {
				c.workList[i].ReportSummary()
			}
    	}
    	
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
                c.workList = append(c.workList, Workload{"CREATE1M", 1, 0, 0, 0, 1000000, true})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "READ 10M"
                c.workList = append(c.workList, Workload{"READ1M", 0, 1, 0, 0, 1000000, true})
                c.reInitSetup = false
                conf = append(conf, c)
        }

//CONF2 CREATE 5M --> CREATE(5M) AND UPDATE (5M)
        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{"CREATE 500K", 1, 0, 0, 0, 500000, false})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE 5M AND UPDATE 5M"
                c.workList = append(c.workList, Workload{"C500KU500K", 0.5, 0, 0.5, 0, 2000000, true})
                c.reInitSetup = false
                conf = append(conf, c)
        }

//CONF3 CREATE 5M --> CREATE(5M) AND READ(5M) -> UPDATE(5M) AND READ(5M)        

        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{"C500K", 1, 0, 0, 0, 500000, false})
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE 5M READ 5M"
                c.workList = append(c.workList, Workload{"C30U30R40", 0.3, 0.4, 0.3, 0, 3000000, true})
                c.reInitSetup = false
                conf = append(conf, c)
        }

*/
//CONF4 CREATE(5M) --> CREATE(5M) AND READ(5M) IN PARALLEL 
//      --> UPDATE(5M) AND READ(5M) IN PARALLEL

        {
                var c BenchConf
                var w Workload
                c.name = "CREATE 5M"
                c.reInitSetup = true

                w.Init("C500K", 1, 0, 0, 0, 300000, false)
                c.workList = append(c.workList, w)  

                conf = append(conf, c)
        }

        {
                var c BenchConf
                var w Workload

                c.name = "CREATE 5M READ 5M PARALLEL"
                c.reInitSetup = false

                w.Init("C1M_P", 1, 0, 0, 0, 300000, true)
                c.workList = append(c.workList, w)  
                w.Init("U1M_P", 0, 0, 1, 0, 300000, true)
                c.workList = append(c.workList, w)  
                w.Init("R1M_P", 0, 1, 0, 0, 300000, true)
                c.workList = append(c.workList, w)  
                w.Init("D1M_P", 0, 0, 0, 1, 100000, true)
                c.workList = append(c.workList, w)  

                conf = append(conf, c)
        }
        
/*
//CONF5 CREATE(5M) --> CREATE(5M) AND READ (5M) 4 THREADS

        {
                var c BenchConf
                c.name = "CREATE 5M"
                c.workList = append(c.workList, Workload{"CREATE5M", 1, 0, 0, 0, 500000, false})  
                c.reInitSetup = true
                conf = append(conf, c)
        }

        {
                var c BenchConf
                c.name = "CREATE(5M) AND READ (5M) 4 THREADS"
                c.workList = append(c.workList, Workload{"CREATE5M", 1, 0, 0, 0, 500000, true})
                c.workList = append(c.workList, Workload{"UPDATE5M", 0, 0, 1, 0, 500000, true})
                c.workList = append(c.workList, Workload{"READ5M_1", 0, 1, 0, 0, 500000, true})
//                c.workList = append(c.workList, Workload{"READ25M", 0, 1, 0, 0, 25000000, true})
//                c.workList = append(c.workList, Workload{"READ35M", 0, 1, 0, 0, 25000000, true})
//                c.workList = append(c.workList, Workload{"READ45M", 0, 1, 0, 0, 25000000, true})
                c.reInitSetup = false
                conf = append(conf, c)
        }
*/       
        return conf

}

