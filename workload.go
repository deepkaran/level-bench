package main

import (
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

func (w *Workload) Init(name string, ratioCreate float64, ratioRead float64, ratioUpdate float64,
	ratioDelete float64, totalOps int64, reportStats bool) {

	w.name = name
	w.ratioCreate = ratioCreate
	w.ratioRead = ratioRead
	w.ratioUpdate = ratioUpdate
	w.ratioDelete = ratioDelete
	w.totalOps = totalOps
	w.reportStats = reportStats

}

func (w *Workload) RunWorkload(db DBInfo, wg *sync.WaitGroup) {

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
				log.Fatalf("DB Error in Put : %v", err)
			}
			if w.reportStats {
				s := StatPacket{CREATE, elapsed}
				statAdd <- s
			}
			p := StorePacket{CREATE, k, false}
			storeRequest <- p

		case READ:
			p := StorePacket{READ, "", false}
			storeRequest <- p
			p = <-storeResponse

			_, elapsed, err := db.Get(p.key)
			if err != nil {
				log.Fatalf("DB Error in Put : %v", err)
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
				log.Fatalf("DB Error in Put : %v", err)
			}
			if w.reportStats {
				s := StatPacket{UPDATE, elapsed}
				statAdd <- s
			}

		case DELETE:
			p := StorePacket{DELETE, "", false}
			storeRequest <- p
			p = <-storeResponse

			elapsed, err := db.Delete(p.key)
			if err != nil {
				log.Fatalf("DB Error in Delete : %v", err)
			}
			if w.reportStats {
				s := StatPacket{DELETE, elapsed}
				statAdd <- s
			}

		case NOOP:
			time.Sleep(time.Microsecond * 10)
		}
	}
	log.Printf("Finished Workload %s", w.name)

}
