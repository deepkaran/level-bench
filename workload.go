package main

import (
	"log"
	"math/rand"
	"time"
	"sync"
    "code.google.com/p/plotinum/plot"
    "code.google.com/p/plotinum/plotter"
    "image/color"
)

type Workload struct {
	name string
    ratioCreate float64
    ratioRead float64
    ratioUpdate float64
    ratioDelete float64
    totalOps int64
    reportStats bool
}

const (
	CREATE = iota
	READ = iota
	UPDATE = iota
	DELETE = iota
)

func (w *Workload) RunWorkload(db DBInfo, f FileDataSource, wg *sync.WaitGroup) {

	defer wg.Done()

	var timeCreate []time.Duration
	var timeRead []time.Duration
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
    
    w.genReport(timeCreate, timeRead, timeUpdate, timeDelete)

}


func (w *Workload) calcStats(timeInfo []time.Duration, opType string) (plotter.XYs) {
	
    var sum int64 = 0
    var count int64 = 0
	pts := make(plotter.XYs, len(timeInfo))
	
	for i, x := range timeInfo {
		if x.Nanoseconds() > 1 && x.Nanoseconds() < 2000000{		
			sum += x.Nanoseconds()
			count += 1
			pts[i].X = float64(i)
			pts[i].Y = float64(x.Nanoseconds()) / 1000.0
		}
	}

	timeInSecs := float64(sum) / 1000000000

	log.Printf("Total Ops for %s : %d", opType, count)
	log.Printf("Total time taken : %f seconds", timeInSecs)
	log.Printf("Ops per second : %f",  float64(count) / timeInSecs)
	
	return pts
}

func (w *Workload) genReport(timeCreate []time.Duration, timeRead []time.Duration, 
								timeUpdate []time.Duration, timeDelete []time.Duration) {

		
	// Create a new plot, set its title and
    // axis labels.
    p, err := plot.New()
    if err != nil {
    	panic(err)
    }
    
    p.Title.Text = "LevelDB Performance :" + w.name
    p.X.Label.Text = "Data Ops"
    p.Y.Label.Text = "Time in Microseconds"
    // Draw a grid behind the data
    p.Add(plotter.NewGrid())


	if len(timeCreate) > 0 {

		pts := w.calcStats(timeCreate, "CREATE")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ R: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}
	
	if len(timeRead) > 0 {

		pts := w.calcStats(timeRead, "READ")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ B: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}

	if len(timeUpdate) > 0 {

		pts := w.calcStats(timeUpdate, "UPDATE")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ G: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}

    // Save the plot to a PNG file.
    if err := p.Save(20, 12, w.name + ".png"); err != nil {
    	panic(err)
    }

}


