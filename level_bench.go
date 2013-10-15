package main

import (
	"log"
	"sync"
	"time"
) 

var db DBInfo
var fds FileDataSource
var st Store 

type BenchConf struct {
    name string
    workList []Workload
    reInitSetup bool
}

func main() {

	conf := confInit()
	storeRequest =  make(chan Packet)
	storeResponse = make(chan Packet)
	
	go st.StoreKeeper()
    
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
	st.Init()
}

func confInit() ([] BenchConf) {

	var conf []BenchConf

//CONF1 CREATE(5M) --> READ(5) --> CREATE(5M)/READ(5M)/UPDATE(5M)/DELETE(2M) IN PARALLEL 

        {
                var c BenchConf
                var w Workload
                c.name = "CREATE 5M"
                c.reInitSetup = true

                w.Init("C5M_I", 1, 0, 0, 0, 5000000, false)
                c.workList = append(c.workList, w)  

                conf = append(conf, c)
        }

        {
                var c BenchConf
                var w Workload
                c.name = "READ 5M"
                c.reInitSetup = false

                w.Init("R5M_I", 0, 1, 0, 0, 5000000, false)
                c.workList = append(c.workList, w)  

                conf = append(conf, c)
        }

        {
                var c BenchConf
                var w Workload

                c.name = "CREATE(5M)/READ(5M)/UPDATE(5M)/DELETE(2M) PARALLEL"
                c.reInitSetup = false

                w.Init("C1M_P", 1, 0, 0, 0, 5000000, true)
                c.workList = append(c.workList, w)  
                w.Init("U1M_P", 0, 0, 1, 0, 5000000, true)
                c.workList = append(c.workList, w)  
                w.Init("R1M_P", 0, 1, 0, 0, 5000000, true)
                c.workList = append(c.workList, w)  
                w.Init("D1M_P", 0, 0, 0, 1, 2000000, true)
                c.workList = append(c.workList, w)  

                conf = append(conf, c)
        }
    
        return conf

}

