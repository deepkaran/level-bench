package main

import (
	"log"
	"sync"
	"time"
)

var db DBInfo
var rs RandomSource
var st Store
var stat Stats
var stop bool

type BenchConf struct {
	name        string
	workList    []Workload
	reInitSetup bool
	runSecs     int64
}

func main() {

	conf := confInit()
	storeRequest = make(chan StorePacket)
	storeResponse = make(chan StorePacket)

	statAdd = make(chan StatPacket, 100)
	dataSource = make(chan DataPacket, 100)

	go st.StoreKeeper()
	go stat.StatsManager()

	rs.Init()
	go rs.GenData()

	for _, c := range conf {
		if c.reInitSetup {
			reInitSetup()
		}
		var wg sync.WaitGroup
		stop = false
		log.Printf("Picked up conf %s", c.name)
		for i := range c.workList {
			wg.Add(1)
			go c.workList[i].RunWorkload(db, &wg)

		}
		if c.runSecs > 0 {
			select {
			case <-time.After(time.Duration(c.runSecs) * 1000 * time.Millisecond):
				stop = true
			}
		} else {
			stop = true
		}

		wg.Wait()
	}

	stat.ReportSummary(true)
}

func reInitSetup() {
	db.Close()
	db.Init("bench")
	st.Init()
}

func confInit() []BenchConf {

	var conf []BenchConf

	{
		var c BenchConf
		var w Workload
		c.name = "CREATE_INIT"
		c.reInitSetup = true

		w.Init("C_I", 1, 0, 0, 0, 500000, false)
		c.workList = append(c.workList, w)

		conf = append(conf, c)
	}

	/*
	   {
	           var c BenchConf
	           var w Workload
	           c.name = "READ_INIT"
	           c.reInitSetup = false

	           w.Init("R_I", 0, 1, 0, 0, 1000000, true)
	           c.workList = append(c.workList, w)

	           conf = append(conf, c)
	   }
	*/

	{
		var c BenchConf
		var w Workload

		c.name = "CREATE_READ_UPDATE_DELETE_PARALLEL"
		c.reInitSetup = false
		c.runSecs = 60

		w.Init("W1_P", 1, 0, 0, 0, 0, true)
		c.workList = append(c.workList, w)
		w.Init("W2_P", 0, 1, 0, 0, 0, true)
		c.workList = append(c.workList, w)
		w.Init("W3_P", 0, 0, 1, 0, 0, true)
		c.workList = append(c.workList, w)
		w.Init("W4_P", 0, 0, 0, 0.1, 0, true)
		c.workList = append(c.workList, w)

		conf = append(conf, c)
	}

	return conf

}
