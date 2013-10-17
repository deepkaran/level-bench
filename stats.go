package main

import (
	"time"
	"log"
    "code.google.com/p/plotinum/plot"
    "code.google.com/p/plotinum/plotter"
    "image/color"
    "github.com/patrick-higgins/summstat"
)

type Stats struct {
	timeCreate []time.Duration
	timeRead []time.Duration
	timeUpdate []time.Duration 
	timeDelete []time.Duration
}


type StatPacket struct {
	statOp int
	latency time.Duration
}

var statAdd chan StatPacket

func (s *Stats) StatsManager() {

	for {
		p := <- statAdd
		
		switch(p.statOp) {
		
		case CREATE:
			s.timeCreate = append(s.timeCreate, p.latency)
				
		case READ:
			s.timeRead =  append(s.timeRead, p.latency)
			
		case UPDATE:
			s.timeUpdate =  append(s.timeUpdate, p.latency)
			
		case DELETE:
			s.timeDelete = append(s.timeDelete, p.latency)
			
		}
	}

}

func (s *Stats) calcStats(timeInfo []time.Duration, opType string, drawPlot bool) {
	
    var sum int64 = 0
	pts := make(plotter.XYs, len(timeInfo))
	stats := summstat.NewStats()
	
	for i, x := range timeInfo {
		if x.Nanoseconds() > 1 && x.Nanoseconds() < 5000000{		
			pts[i].X = float64(i)
			pts[i].Y = float64(x.Nanoseconds()) / 1000.0
		}
		sample := summstat.Sample(float64(x.Nanoseconds()) / 1000.0)
		stats.AddSample(sample)
		sum += x.Nanoseconds()
	}
	
	log.Println("**********************************************")
	log.Printf("Stats for %s", opType)

	log.Printf("Total Ops for %s : %d", opType, stats.Count())
	log.Printf("Total time taken : %f seconds", float64(sum) / 1000000000)

	for _, percentile := range []float64{0.8, 0.9, 0.95, 0.99} {
		value := stats.Percentile(percentile)
		log.Printf("Ops per second %vth percentile: %v\n", percentile*100, 1000000 / value)
	}
	mean := stats.Mean()
	log.Printf("Ops per second Mean: %v\n", 1000000 / mean)	

	if drawPlot {
		s.drawPlot(pts, opType)
	}
}

func (s *Stats) drawPlot(pts plotter.XYs, opType string) {


	// Create a new plot, set its title and
    // axis labels.
    p, err := plot.New()
    if err != nil {
    	panic(err)
    }
    
    p.Title.Text = "LevelDB Performance" + opType
    p.X.Label.Text = "Number of Operations"
    p.Y.Label.Text = "Latency in Microseconds"
    // Draw a grid behind the data
    p.Add(plotter.NewGrid())
    
	// Make a scatter plotter and set its style.	
	sc, e := plotter.NewScatter(pts)
	if e != nil {
		panic(e)
    }
    
	sc.GlyphStyle.Color = color.RGBA{ R: 128, A: 255}
	sc.GlyphStyle.Radius = 1        
	p.Add(sc)

   // Save the plot to a PNG file.
    if err := p.Save(20, 12, opType + ".png"); err != nil {
    	panic(err)
    }

}



func (s *Stats) ReportSummary(drawPlot bool) {

	if len(s.timeCreate) > 0 {
		s.calcStats(s.timeCreate, "CREATE", drawPlot)
	}
	if len(s.timeRead) > 0 {
		s.calcStats(s.timeRead, "READ", drawPlot)
	}

	if len(s.timeUpdate) > 0 {
		s.calcStats(s.timeUpdate, "UPDATE", drawPlot)
	}

	if len(s.timeDelete) > 0 {
		s.calcStats(s.timeDelete, "DELETE", drawPlot)
	}

}

