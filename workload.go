package main

import (
	"log"
	"time"
	"sync"
    "code.google.com/p/plotinum/plot"
    "code.google.com/p/plotinum/plotter"
    "image/color"
    "github.com/patrick-higgins/summstat"
)

type Workload struct {
	name string
    ratioCreate float64
    ratioRead float64
    ratioUpdate float64
    ratioDelete float64
    totalOps int64
    reportStats bool
    stats Stats
}

type Stats struct {
	timeCreate []time.Duration
	timeRead []time.Duration
	timeUpdate []time.Duration 
	timeDelete []time.Duration
}

const (
	CREATE = iota
	READ = iota
	UPDATE = iota
	DELETE = iota
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


func (w *Workload) RunWorkload(db DBInfo, f FileDataSource, wg *sync.WaitGroup) {

	defer wg.Done()

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

	var i int64
	
    for i = 0; i < w.totalOps; i++ {
    
        switch(opList[i%10]) {
        
        case CREATE:
			k, v := f.Next()
            elapsed, err := db.Set(k, v)
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        w.stats.timeCreate = append(w.stats.timeCreate, elapsed)
		    p := Packet{CREATE, k, false}
		    storeRequest <- p
		    <- storeResponse

        case READ:
		    p := Packet{READ, "", false}
		    storeRequest <- p
		    p = <- storeResponse
		    
            _, elapsed, err := db.Get(p.key)
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        w.stats.timeRead = append(w.stats.timeRead, elapsed)
        
        case UPDATE:

		    p := Packet{READ, "", false}
		    storeRequest <- p
		    p = <- storeResponse

            v, elapsed, err := db.Get(p.key)
	        if err != nil {
	            log.Fatalf("DB Error in Get : %v", err)
	        }
            elapsed, err = db.Set(p.key, v)
	        if err != nil {
	            log.Fatalf("DB Error in Put : %v", err)
	        }
	        w.stats.timeUpdate = append(w.stats.timeUpdate, elapsed)
        
        case DELETE:
		    p := Packet{DELETE, "", false}
		    storeRequest <- p
		    p = <- storeResponse

            elapsed, err := db.Delete(p.key)
	        if err != nil {
	            log.Fatalf("DB Error in Delete : %v", err)
	        }
	        w.stats.timeDelete = append(w.stats.timeDelete, elapsed)
       
        }
    }    
   

}

func (w *Workload) calcStats(timeInfo []time.Duration, opType string) (plotter.XYs) {
	
    var sum int64 = 0
	pts := make(plotter.XYs, len(timeInfo))
	stats := summstat.NewStats()
	
	for i, x := range timeInfo {
		if x.Nanoseconds() > 1 && x.Nanoseconds() < 3000000{		
			pts[i].X = float64(i)
			pts[i].Y = float64(x.Nanoseconds()) / 1000.0
		}
		sample := summstat.Sample(float64(x.Nanoseconds()) / 1000.0)
		stats.AddSample(sample)
		sum += x.Nanoseconds()
	}
	
	log.Println("**********************************************")
	log.Printf("Stats for Workload %s", w.name)

	log.Printf("Total Ops for %s : %d", opType, stats.Count())
	log.Printf("Total time taken : %f seconds", float64(sum) / 1000000000)

	for _, percentile := range []float64{0.8, 0.9, 0.95, 0.99} {
		value := stats.Percentile(percentile)
		log.Printf("Ops per second %vth percentile: %v\n", percentile*100, 1000000 / value)
	}
	mean := stats.Mean()
	log.Printf("Ops per second Mean: %v\n", 1000000 / mean)	

	return pts
}

func (w *Workload) ReportSummary() {

		
	// Create a new plot, set its title and
    // axis labels.
    p, err := plot.New()
    if err != nil {
    	panic(err)
    }
    
    p.Title.Text = "LevelDB Performance :" + w.name
    p.X.Label.Text = "Number of Operations"
    p.Y.Label.Text = "Latency in Microseconds"
    // Draw a grid behind the data
    p.Add(plotter.NewGrid())

	if len(w.stats.timeCreate) > 0 {

		pts := w.calcStats(w.stats.timeCreate, "CREATE")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ R: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}
	
	if len(w.stats.timeRead) > 0 {

		pts := w.calcStats(w.stats.timeRead, "READ")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ B: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}

	if len(w.stats.timeUpdate) > 0 {

		pts := w.calcStats(w.stats.timeUpdate, "UPDATE")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ G: 128, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}

	if len(w.stats.timeDelete) > 0 {

		pts := w.calcStats(w.stats.timeDelete, "DELETE")
		// Make a scatter plotter and set its style.	
		s, err := plotter.NewScatter(pts)
		if err != nil {
			panic(err)
    	}
    
		s.GlyphStyle.Color = color.RGBA{ R: 255, A: 255}
		s.GlyphStyle.Radius = 1        
		p.Add(s)
	}


    // Save the plot to a PNG file.
    if err := p.Save(20, 12, w.name + ".png"); err != nil {
    	panic(err)
    }

}


