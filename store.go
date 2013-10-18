package main

import (
	"math/rand"
)

type Store struct {
	keyList     []string
	keyCount    int64
	deletedKeys map[string]bool
}

type StorePacket struct {
	storeOp int
	key     string
	ok      bool
}

var storeRequest chan StorePacket
var storeResponse chan StorePacket

func (s *Store) Init() {

	s.keyList = nil
	s.keyCount = 0
	s.deletedKeys = make(map[string]bool)

}

func (s *Store) StoreKeeper() {

	for {
		p := <-storeRequest

		switch p.storeOp {

		case CREATE:
			s.keyList = append(s.keyList, p.key)
			s.keyCount++
			p.ok = true

		case READ:
			p.key = s.generateValidRandomKey()
			p.ok = true
			storeResponse <- p

		case DELETE:
			p.key = s.generateValidRandomKey()
			s.deletedKeys[p.key] = true
			p.ok = true
			storeResponse <- p
		}
	}

}

func (s *Store) generateValidRandomKey() (key string) {

	validKey := false
	key = ""

	if s.keyCount == 0 {
		return key
	}

	for validKey == false {
		i := rand.Int63n(s.keyCount)
		key = s.keyList[i]
		if s.deletedKeys[key] {
			validKey = true
		} else {
			validKey = true
		}
	}
	return key
}
