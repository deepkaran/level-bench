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
			go runWorkload(db, w, fds, &wg)
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
//CONF1 CREATE 100K --> READ 100K	
	{
		var c BenchConf
		c.name = "CREATE 100K"
		c.workList = append(c.workList, Workload{1, 0, 0, 0, 100000})
		c.reInitSetup = true
		conf = append(conf, c)
	}

	{
		var c BenchConf
		c.name = "READ 100K"
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 100000})
		c.reInitSetup = false
		conf = append(conf, c)
	}
*/

/*
//CONF2 CREATE 1M --> READ 1M	
	c.name = "CREATE 1M"
	c.workList = append(c.workList, Workload{1, 0, 0, 0, 1000000})
	c.reInitSetup = true
	conf = append(conf, c)
	c = nil

	c.name = "READ 1M"
	c.workList = append(c.workList, Workload{0, 1, 0, 0, 1000000})
	c.reInitSetup = false
	conf = append(conf, c)
	c = nil

//CONF3 CREATE 500K --> CREATE(500K) AND READ(500K) -> UPDATE(500K) AND READ(500K)	
	c.name = "CREATE 500K"
	c.workList = append(c.workList, Workload{1, 0, 0, 0, 500000})
	c.reInitSetup = true
	conf = append(conf, c)
	c = nil

	c.name = "CREATE 500K READ 500K"
	c.workList = append(c.workList, Workload{0.5, 0.5, 0, 0, 1000000})
	c.reInitSetup = false
	conf = append(conf, c)
	c = nil

	c.name = "UPDATE 500K READ 500K"
	c.workList = append(c.workList, Workload{0, 0.5, 0.5, 0, 1000000})
	c.reInitSetup = false
	conf = append(conf, c)
	c = nil
*/
/*
//CONF4 CREATE(500K) --> CREATE(500K) AND READ(500K) IN PARALLEL 
//	--> UPDATE(500K) AND READ(500K) IN PARALLEL

	{
		var c BenchConf
		c.name = "CREATE 500K"
		c.workList = append(c.workList, Workload{1, 0, 0, 0, 500000})	
		c.reInitSetup = true
		conf = append(conf, c)
	}

	{
		var c BenchConf
		c.name = "CREATE 500K READ 500K"
		c.workList = append(c.workList, Workload{1, 0, 0, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.reInitSetup = false
		conf = append(conf, c)
	}
	
	
	{
		var c BenchConf
		c.name = "UPDATE 500K READ 500K"
		c.workList = append(c.workList, Workload{0, 0, 1, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.reInitSetup = false
		conf = append(conf, c)
	}
*/

//CONF5 CREATE(500K) --> CREATE(500K) AND READ (500K) 4 THREADS

	{
		var c BenchConf
		c.name = "CREATE 500K"
		c.workList = append(c.workList, Workload{1, 0, 0, 0, 500000})	
		c.reInitSetup = true
		conf = append(conf, c)
	}

	{
		var c BenchConf
		c.name = "CREATE(500K) AND READ (500K) 4 THREADS"
		c.workList = append(c.workList, Workload{1, 0, 0, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.workList = append(c.workList, Workload{0, 1, 0, 0, 500000})
		c.reInitSetup = false
		conf = append(conf, c)
	}
	
	return conf

}
