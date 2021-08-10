package donutdb

import "github.com/psanford/sqlite3vfs"

type lockManager interface {
	lock(sqlite3vfs.LockType) error
	unlock(sqlite3vfs.LockType) error
	close() error
	level() sqlite3vfs.LockType
	checkReservedLock() (bool, error)
}
