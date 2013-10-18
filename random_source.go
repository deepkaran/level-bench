package main

import (
	"bufio"
	"log"
	"math/rand"
	"os"
	"strconv"
)

type RandomSource struct {
	masterKeys   []string
	nKeys        int
	masterValues []string
	nValues      int
}

type DataPacket struct {
	key   string
	value string
}

var dataSource chan DataPacket

func (rs *RandomSource) Init() {

	var err error
	rs.masterKeys, err = readLines("master/keys.dat")
	if err != nil {
		log.Fatalf("Unable to read master key file. Error: %v", err)
	}
	rs.nKeys = len(rs.masterKeys)

	rs.masterValues, err = readLines("master/values.dat")
	if err != nil {
		log.Fatalf("Unable to read master value file. Error: %v", err)
	}
	rs.nValues = len(rs.masterValues)

}

func (rs *RandomSource) GenData() {

	for {
		var p DataPacket
		p.key = rs.masterKeys[rand.Intn(rs.nKeys)] + "_" + strconv.Itoa(rand.Intn(100000)) + strconv.Itoa(rand.Intn(100000))
		p.value = rs.masterValues[rand.Intn(rs.nValues)]
		dataSource <- p
	}

}

func (rs *RandomSource) OneValue() string {

	return rs.masterValues[rand.Intn(rs.nValues)]

}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}
