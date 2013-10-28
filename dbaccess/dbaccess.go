package dbaccess

import (
	"time"
)

type DBAccess interface {
	Init(string)
	Set(string, string) (time.Duration, error)
	Get(string) (string, time.Duration, error)
	Delete(string) (time.Duration, error)
	Close()
}
