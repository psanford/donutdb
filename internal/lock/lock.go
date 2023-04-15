package lock

import "github.com/psanford/sqlite3vfs"

type LockManager interface {
	Lock(sqlite3vfs.LockType) error
	Unlock(sqlite3vfs.LockType) error
	Close() error
	Level() sqlite3vfs.LockType
	CheckReservedLock() (bool, error)
}
