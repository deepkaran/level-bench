package main

import (
	. "github.com/deepkaran/level-bench/dbaccess"
	"log"
	"sync"
	"time"
)

type Workload struct {
	name        string
	ratioCreate float64
	ratioRead   float64
	ratioUpdate float64
	ratioDelete float64
	totalOps    int64
	reportStats bool
}

const (
	CREATE = iota
	READ   = iota
	UPDATE = iota
	DELETE = iota
	NOOP   = iota
)

var opName []string

func (w *Workload) Init(name string, ratioCreate float64, ratioRead float64, ratioUpdate float64,
	ratioDelete float64, totalOps int64, reportStats bool) {

	w.name = name
	w.ratioCreate = ratioCreate
	w.ratioRead = ratioRead
	w.ratioUpdate = ratioUpdate
	w.ratioDelete = ratioDelete
	w.totalOps = totalOps
	w.reportStats = reportStats
	opName = []string{"CREATE", "READ", "UPDATE", "DELETE"}
}

func (w *Workload) RunWorkload(db DBAccess, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Printf("Starting Workload %s", w.name)

	var opList []int

	for c := 0.0; c < w.ratioCreate; {
		opList = append(opList, CREATE)
		c += 0.1
	}

	for c := 0.0; c < w.ratioRead; {
		opList = append(opList, READ)
		c += 0.1
	}

	for c := 0.0; c < w.ratioUpdate; {
		opList = append(opList, UPDATE)
		c += 0.1
	}

	for c := 0.0; c < w.ratioDelete; {
		opList = append(opList, DELETE)
		c += 0.1
	}

	for len(opList) < 10 {
		opList = append(opList, NOOP)
	}

	var i int64
	for i = 0; i < w.totalOps || !stop; i++ {

		switch opList[i%10] {

		case CREATE:
			d := <-dataSource
			k := d.key
			v := d.value
			elapsed, err := db.Set(k, v)
			if err != nil {
				log.Printf("DB Error in Create : %v", err)
			}
			if w.reportStats {
				s := StatPacket{CREATE, elapsed}
				statAdd <- s
			}
			p := StorePacket{CREATE, k, false}
			storeRequest <- p
			//log.Printf("CREATE \nKEY | %s \nVALUE | %s", k, v)
			if fdb, ok := db.(*ForestDB); ((i+1)%50000 == 0) && ok {
				fdb.Compact()
			}
			if cs, ok := db.(*CouchStore); ((i+1)%50000 == 0) && ok {
				cs.Compact()
			}

		case READ:

			if fdb, ok := db.(*ForestDB); ok && fdb.WaitForCompaction {
				time.Sleep(time.Microsecond * 1)
				continue
			}
			if cs, ok := db.(*CouchStore); ok && cs.WaitForCompaction {
				time.Sleep(time.Microsecond * 1)
				continue
			}

			p := StorePacket{READ, "", false}
			storeRequest <- p
			p = <-storeResponse

			v, elapsed, err := db.Get(p.key)
			if v == "" {
				log.Printf("\nERROR!!!! DB RETURNED EMPTY VALUE!!!! \nKEY - %s \nVALUE - %s", p.key, v)
			}
			//log.Printf("READ \nKEY | %s \nVALUE | %s", p.key, v)
			if err != nil {
				log.Printf("DB Error in Read : %v", err)
			}
			if w.reportStats {
				s := StatPacket{READ, elapsed}
				statAdd <- s
			}

		case UPDATE:

			p := StorePacket{READ, "", false}
			storeRequest <- p
			p = <-storeResponse

			v := rs.OneValue()
			elapsed, err := db.Set(p.key, v)
			if err != nil {
				log.Printf("DB Error in Update : %v", err)
			}
			if w.reportStats {
				s := StatPacket{UPDATE, elapsed}
				statAdd <- s
			}
			//log.Printf("UPDATE \nKEY | %s \nVALUE | %s", p.key, v)

		case DELETE:
			p := StorePacket{DELETE, "", false}
			storeRequest <- p
			p = <-storeResponse

			elapsed, err := db.Delete(p.key)
			if err != nil {
				log.Printf("DB Error in Delete : %v", err)
			}

			if w.reportStats {
				s := StatPacket{DELETE, elapsed}
				statAdd <- s
			}
			//log.Printf("DELETE \nKEY | %s", p.key)

		case NOOP:
			time.Sleep(time.Microsecond * 10)
		}

	}
	log.Printf("Finished Workload %s", w.name)

}
