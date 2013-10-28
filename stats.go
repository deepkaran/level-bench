package main

import (
	"code.google.com/p/plotinum/plot"
	"code.google.com/p/plotinum/plotter"
	"github.com/patrick-higgins/summstat"
	"image/color"
	"log"
	"time"
)

type TimeInfo []time.Duration

type Stats [4]TimeInfo

type StatPacket struct {
	statOp  int
	latency time.Duration
}

var statAdd chan StatPacket

func (s *Stats) StatsManager() {

	for {
		p := <-statAdd
		s[p.statOp] = append(s[p.statOp], p.latency)
	}

}

func (s *Stats) calcStats(timeInfo []time.Duration, opType string) {

	if len(timeInfo) == 0 {
		return
	}

	var sum int64 = 0
	stats := summstat.NewStats()

	for _, x := range timeInfo {
		sample := summstat.Sample(float64(x.Nanoseconds()) / 1000.0)
		stats.AddSample(sample)
		sum += x.Nanoseconds()
	}

	log.Println("**********************************************")
	log.Printf("Stats for %s", opType)

	log.Printf("Total Ops for %s : %d", opType, stats.Count())
	log.Printf("Total time taken : %f seconds", float64(sum)/1000000000)

	for _, percentile := range []float64{0.8, 0.9, 0.95, 0.99} {
		value := stats.Percentile(percentile)
		log.Printf("Ops per second %vth percentile: %v\n", percentile*100, 1000000/value)
	}
	mean := stats.Mean()
	log.Printf("Ops per second Mean: %v\n", 1000000/mean)

}

func (s *Stats) drawPlot(timeInfo []time.Duration, opType string) {

	if len(timeInfo) == 0 {
		return
	}

	pts := make(plotter.XYs, len(timeInfo))

	for i, x := range timeInfo {
		if x.Nanoseconds() > 1 && x.Nanoseconds() < 10000000 {
			pts[i].X = float64(i)
			pts[i].Y = float64(x.Nanoseconds()) / 1000.0
		}
	}

	// Create a new plot, set its title and
	// axis labels.
	p, err := plot.New()
	if err != nil {
		panic(err)
	}

	p.Title.Text = opType + "Performance"
	p.X.Label.Text = "Number of Operations"
	p.Y.Label.Text = "Latency in Microseconds"
	// Draw a grid behind the data
	p.Add(plotter.NewGrid())
	// Make a scatter plotter and set its style.
	sc, e := plotter.NewScatter(pts)
	if e != nil {
		panic(e)
	}

	sc.GlyphStyle.Color = color.RGBA{R: 128, A: 255}
	sc.GlyphStyle.Radius = 1
	p.Add(sc)

	// Save the plot to a PNG file.
	if err := p.Save(20, 12, opType+".png"); err != nil {
		panic(err)
	}

}

func (s *Stats) ReportSummary(drawPlot bool) {

	for i, o := range opName {
		s.calcStats(s[i], o)
	}

	if drawPlot {
		for i, o := range opName {
			s.drawPlot(s[i], o)
		}
	}

}
