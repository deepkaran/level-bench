package main

import (
	"math/rand"
)

type RandomSource struct {
	keySize  int
	valSize  int
	src      rand.Source
	valStash [5000]string
	stashLen int
}

type DataPacket struct {
	key   string
	value string
}

var dataSource chan DataPacket

func (rs *RandomSource) Init() {

	rs.keySize = 10
	rs.valSize = 10
	rs.src = rand.NewSource(1028890720402726901)
	rs.stashLen = 5000
	rs.initValueStash()
}

func (rs *RandomSource) randString(n int) string {
	var p []byte
	todo := n
	for {
		val := rs.src.Int63()
		for i := 0; i < 8; i++ {
			offset := (val & 0xff) % 50
			p = append(p, byte(33+offset)) //ASCII RANGE 33 - 122
			todo--
			if todo == 0 {
				return string(p)
			}
			val >>= 8
		}
	}
}

func (rs *RandomSource) GenData() {

	for {
		var p DataPacket
		p.key = rs.randString(rs.keySize)
		p.value = rs.randString(rs.valSize)
		dataSource <- p
	}

}

func (rs *RandomSource) initValueStash() {

	for i := 0; i < rs.stashLen; i++ {

		rs.valStash[i] = rs.randString(rs.valSize)
	}
}

func (rs *RandomSource) OneValue() string {

	return rs.valStash[rand.Intn(rs.stashLen)]
}
