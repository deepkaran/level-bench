package main

import (
	"math/rand"
)

type Store struct {
	keyList       []string
	MAX_KEY_COUNT int64
	keyCount      int64
	currKeyIndex  int64
}

type StorePacket struct {
	storeOp int
	key     string
	ok      bool
}

var storeRequest chan StorePacket
var storeResponse chan StorePacket

func (s *Store) Init() {

	s.MAX_KEY_COUNT = 5000000
	s.keyList = make([]string, s.MAX_KEY_COUNT)
	s.keyCount = 0
	s.currKeyIndex = 0

}

func (s *Store) StoreKeeper() {

	for {
		p := <-storeRequest

		switch p.storeOp {

		case CREATE:

			s.keyList[s.currKeyIndex] = p.key
			s.currKeyIndex++
			if s.keyCount < s.MAX_KEY_COUNT {
				s.keyCount++
			}
			if s.currKeyIndex == s.MAX_KEY_COUNT {
				s.currKeyIndex = s.MAX_KEY_COUNT / 2
			}

		case READ:
			p.key, _ = s.generateValidRandomKey(p.storeOp)
			p.ok = true
			storeResponse <- p

		case DELETE:
			var idx int64
			p.key, idx = s.generateValidRandomKey(p.storeOp)
			s.deleteFromStore(idx)
			p.ok = true
			storeResponse <- p
		}
	}

}

func (s *Store) generateValidRandomKey(op int) (key string, index int64) {

	key = ""
	if s.keyCount == 0 {
		return key, 0
	}

	var i int64
	if op == READ {
		i = rand.Int63n(s.keyCount)
	} else if op == DELETE {
		i = s.keyCount/2 + rand.Int63n(s.keyCount/2)
	}
	key = s.keyList[i]

	return key, i
}

func (s *Store) deleteFromStore(index int64) {

	s.keyList[index] = s.keyList[s.keyCount-1]
	s.keyCount--
	s.currKeyIndex = s.keyCount
}
