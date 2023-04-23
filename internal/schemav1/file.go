package schemav1

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/changelog"
	"github.com/psanford/donutdb/internal/dynamo"
	"github.com/psanford/donutdb/internal/lock"
	"github.com/psanford/sqlite3vfs"
)

type File struct {
	dataRowKey string
	rawName    string
	randID     string
	sectorSize int64
	closed     bool

	changeLogWriter *json.Encoder
	db              *dynamodb.DynamoDB
	table           string

	cachedSize int64

	lockManager lock.LockManager
}

func FileFromMeta(meta *dynamo.FileMetaV1V2, table, ownerID string, db *dynamodb.DynamoDB, changeLogWriter *json.Encoder) (*File, error) {

	if meta.MetaVersion > 1 {
		return nil, fmt.Errorf("cannot instanciate schemav1 file for MetaVersion=%d", meta.MetaVersion)
	}

	f := &File{
		dataRowKey:      dynamo.FileDataPrefix + meta.RandID + "-" + meta.OrigName,
		rawName:         meta.OrigName,
		randID:          meta.RandID,
		sectorSize:      meta.SectorSize,
		table:           table,
		db:              db,
		changeLogWriter: changeLogWriter,

		lockManager: lock.NewGlobalLockManger(db, table, meta.LockRowKey, ownerID),
	}
	return f, nil
}

func (f *File) Close() error {
	f.closed = true
	err := f.lockManager.Close()
	if err != nil {
		return err
	}
	return nil
}

func (f *File) ReadAt(p []byte, off int64) (retN int, retErr error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "ReadAtStart",
			FName:  f.rawName,
			Off:    off,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:     time.Now(),
				Action: "ReadAtComplete",
				P:      p,
				Off:    off,
				FName:  f.rawName,

				RetCount: retN,
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	if f.closed {
		return 0, os.ErrClosed
	}

	firstSector := f.sectorForPos(off)

	fileSize, err := f.FileSize()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := f.sectorForPos(lastByte)

	var sect Sector
	iter := f.newSectorIterator(&sect, firstSector, lastSector, f.sectorSize)

	var (
		n              int
		first          = true
		iterCount      int
		prevSeenSize   = f.sectorSize
		prevSeenOffset = int64(0)
	)
	for iter.Next() {
		if prevSeenSize != f.sectorSize {
			return n, fmt.Errorf("non-full sector detected in the middle of a file sector=%d size=%d", prevSeenOffset, prevSeenSize)
		}

		prevSeenSize = int64(len(sect.Data))
		prevSeenOffset = sect.Offset

		if first {
			startIndex := off % f.sectorSize
			n = copy(p, sect.Data[startIndex:])
			first = false
			continue
		}

		nn := copy(p[n:], sect.Data)
		n += nn
		iterCount++
	}
	err = iter.Close()
	if err == dynamo.SectorNotFoundErr && lastByte >= fileSize {
		return n, io.EOF
	} else if err != nil {
		return n, err
	}

	if lastByte >= fileSize {
		return n, io.EOF
	}

	return n, nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "WriteAtStart",
			P:      b,
			Off:    off,
			FName:  f.rawName,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:     time.Now(),
				Action: "WriteAtComplete",
				Off:    off,
				FName:  f.rawName,

				RetCount: n,
				RetError: err,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	if f.closed {
		return 0, os.ErrClosed
	}

	var writeCount int

	oldFileSize, err := f.FileSize()
	if err != nil {
		return writeCount, fmt.Errorf("filesize err: %w", err)
	}

	firstSector := f.sectorForPos(off)

	oldLastSector := f.sectorForPos(oldFileSize)

	defer func() {
		if off+int64(len(b)) > oldFileSize {
			f.cachedSize = off + int64(len(b))
		}
	}()

	secWriter := &SectorWriter{
		F: f,
	}

	lastSectorOffset := (off + int64(len(b))) - ((off + int64(len(b))) % f.sectorSize)

	// if we're writing after the end of the file, we need to fill any intermediary
	// sectors with zeros.
	for sectorStart := oldLastSector; sectorStart < lastSectorOffset; sectorStart += f.sectorSize {
		sectorLastBytePossible := sectorStart + f.sectorSize - 1
		if oldFileSize <= sectorStart {
			// create sector as empty
			err = secWriter.WriteSector(&Sector{
				Offset: sectorStart,
				Data:   make([]byte, f.sectorSize),
			})
			if err != nil {
				return writeCount, fmt.Errorf("fill sector (off=%d) err: %w", sectorStart, err)
			}
		} else if oldFileSize < int64(sectorLastBytePossible) {
			// fill existing sector
			sect, err := f.getSector(sectorStart)
			if err != nil {
				return 0, err
			}
			fill := make([]byte, f.sectorSize-int64(len(sect.Data)))
			sect.Data = append(sect.Data, fill...)
			err = secWriter.WriteSector(sect)
			if err != nil {
				return 0, err
			}
		} else {
			// this is a full sector, don't do anything
		}
	}

	err = secWriter.Flush()
	if err != nil {
		return 0, err
	}

	var sect Sector
	iter := f.newSectorIterator(&sect, firstSector, lastSectorOffset, f.sectorSize)

	var (
		idx      int
		iterDone bool
	)
	for sec := firstSector; sec <= lastSectorOffset; sec += f.sectorSize {
		if iterDone {
			sect = Sector{
				Offset: sec,
				Data:   make([]byte, 0),
			}

		} else {
			if !iter.Next() {
				iterDone = true
				err := iter.Close()
				if err == dynamo.SectorNotFoundErr {
					if sec != lastSectorOffset {

						return writeCount, fmt.Errorf("get sector err: %w", err)
					}
				} else if err != nil {
					return writeCount, fmt.Errorf("get sector err: %w", err)
				}
				sect = Sector{
					Offset: sec,
					Data:   make([]byte, 0),
				}
			}
		}

		var offsetIntoSector int64
		if idx == 0 {
			offsetIntoSector = off % f.sectorSize
		}

		if sect.Offset < lastSectorOffset && int64(len(sect.Data)) < f.sectorSize {
			fill := make([]byte, f.sectorSize-int64(len(sect.Data)))
			sect.Data = append(sect.Data, fill...)
		} else if sect.Offset == lastSectorOffset && len(sect.Data) < int(offsetIntoSector)+len(b) {
			fill := make([]byte, int(offsetIntoSector)+len(b)-len(sect.Data))
			sect.Data = append(sect.Data, fill...)
		}

		n := copy(sect.Data[offsetIntoSector:], b)
		b = b[n:]

		sectCopy := sect
		err = secWriter.WriteSector(&sectCopy)
		if err != nil {
			return writeCount, fmt.Errorf("write sector err: %w", err)
		}
		writeCount += n
		idx++
	}

	if !iterDone {
		err = iter.Close()
		if err != nil {
			return writeCount, fmt.Errorf("get sector err: %w", err)
		}
	}

	err = secWriter.Flush()
	if err != nil {
		return 0, err
	}

	return writeCount, nil
}

func (f *File) SanityCheckSectors() error {
	fileSize, err := f.FileSize()
	if err != nil {
		return err
	}

	lastSector := f.sectorForPos(fileSize)
	var sect Sector
	iter := f.newSectorIterator(&sect, 0, lastSector, f.sectorSize)

	var n int64
	for iter.Next() {
		expectOffset := n * f.sectorSize
		if sect.Offset != int64(expectOffset) {
			iter.Close()
			return fmt.Errorf("sector %d gotOffset=%d expectedOffset=%d", n, sect.Offset, expectOffset)
		}
		n++
	}
	err = iter.Close()
	if err != nil {
		return err
	}

	n--

	if n*f.sectorSize != lastSector {
		return fmt.Errorf("did not reach final sector: last seen n=%d offset=%d expected=%d", n, n*f.sectorSize, lastSector)
	}

	return nil
}

func (f *File) Truncate(size int64) (retErr error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "TruncStart",
			FName:  f.rawName,
			Off:    size,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:       time.Now(),
				Action:   "TruncComplete",
				FName:    f.rawName,
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	fileSize, err := f.FileSize()
	if err != nil {
		return err
	}

	if size >= fileSize {
		return nil
	}

	firstSector := f.sectorForPos(size)

	sect, err := f.getSector(firstSector)
	if err != nil {
		return err
	}

	sect.Data = sect.Data[:size%f.sectorSize]

	secWriter := &SectorWriter{
		F: f,
	}

	secWriter.WriteSector(sect)

	lastSector := f.sectorForPos(fileSize)

	startSector := firstSector + f.sectorSize
	for sectToDelete := startSector; sectToDelete <= lastSector; sectToDelete += f.sectorSize {
		secWriter.DeleteSector(sectToDelete)
	}

	return secWriter.Flush()
}

func (f *File) sectorForPos(pos int64) int64 {
	return pos - (pos % f.sectorSize)
}

func (f *File) Sync(flag sqlite3vfs.SyncType) error {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "SyncStart",
			FName:  f.rawName,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:     time.Now(),
				Action: "SyncComplete",
				FName:  f.rawName,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	return nil
}

func (f *File) FileSize() (retSize int64, retErr error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "FileSizeStart",
			FName:  f.rawName,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:       time.Now(),
				Action:   "FileSizeComplete",
				FName:    f.rawName,
				RetCount: int(retSize),
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	sector, err := f.getLastSector()
	if err == dynamo.SectorNotFoundErr {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	size := sector.Offset + int64(len(sector.Data))

	if size > f.cachedSize {
		f.cachedSize = size
	} else if size < f.cachedSize {
		log.Printf("filesize smaller than cache: real=%d cache=%d", size, f.cachedSize)
	}

	return size, nil
}

func (f *File) Lock(elock sqlite3vfs.LockType) (retErr error) {
	//    UNLOCKED -> SHARED
	//    SHARED -> RESERVED
	//    SHARED -> (PENDING) -> EXCLUSIVE
	//    RESERVED -> (PENDING) -> EXCLUSIVE
	//    PENDING -> EXCLUSIVE

	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:       time.Now(),
			Action:   "LockStart",
			FName:    f.rawName,
			ArgFlags: int(elock),
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:       time.Now(),
				Action:   "LockComplete",
				FName:    f.rawName,
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	curLevel := f.lockManager.Level()

	if elock <= curLevel {
		return nil
	}

	//  (1) We never move from unlocked to anything higher than shared lock.
	if curLevel == sqlite3vfs.LockNone && elock > sqlite3vfs.LockShared {
		return errors.New("invalid lock transition requested")
	}
	//  (2) SQLite never explicitly requests a pendig lock.
	if elock == sqlite3vfs.LockPending {
		return errors.New("invalid Lock() request for state pending")
	}
	//  (3) A shared lock is always held when a reserve lock is requested.
	if elock == sqlite3vfs.LockReserved && curLevel != sqlite3vfs.LockShared {
		return errors.New("can only transition to Reserved lock from Shared lock")
	}

	return f.lockManager.Lock(elock)
}

func (f *File) Unlock(elock sqlite3vfs.LockType) (retErr error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:       time.Now(),
			Action:   "UnlockStart",
			FName:    f.rawName,
			ArgFlags: int(elock),
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			r := changelog.Record{
				TS:       time.Now(),
				Action:   "UnlockComplete",
				FName:    f.rawName,
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	return f.lockManager.Unlock(elock)
}

func (f *File) CheckReservedLock() (retB bool, retErr error) {
	if f.changeLogWriter != nil {
		r := changelog.Record{
			TS:     time.Now(),
			Action: "CheckReservedLockStart",
			FName:  f.rawName,
		}
		f.changeLogWriter.Encode(r)
		defer func() {
			c := 0
			if retB {
				c = 1
			}
			r := changelog.Record{
				TS:       time.Now(),
				Action:   "CheckReservedLockComplete",
				FName:    f.rawName,
				RetCount: c,
				RetError: retErr,
			}
			f.changeLogWriter.Encode(r)
		}()
	}

	return f.lockManager.CheckReservedLock()
}

func (f *File) SectorSize() int64 {
	return f.sectorSize
}

func (f *File) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	c := sqlite3vfs.IocapSafeAppend | sqlite3vfs.IocapSequential
	switch f.sectorSize {
	case 1 << 9:
		c |= sqlite3vfs.IocapAtomic512
	case 1 << 10:
		c |= sqlite3vfs.IocapAtomic1K
	case 1 << 11:
		c |= sqlite3vfs.IocapAtomic2K
	case 1 << 12:
		c |= sqlite3vfs.IocapAtomic4K
	case 1 << 13:
		c |= sqlite3vfs.IocapAtomic8K
	case 1 << 14:
		c |= sqlite3vfs.IocapAtomic16K
	case 1 << 15:
		c |= sqlite3vfs.IocapAtomic32K
	case 1 << 16:
		c |= sqlite3vfs.IocapAtomic64K
	}

	return c
}

func (f *File) CleanupSectors(meta *dynamo.FileMetaV1V2) error {
	lastSec, err := f.getLastSector()
	if err != nil {
		return err
	}

	secWriter := &SectorWriter{
		F: f,
	}

	for sectToDelete := lastSec.Offset; sectToDelete >= 0; sectToDelete -= f.sectorSize {
		secWriter.DeleteSector(sectToDelete)
	}

	err = secWriter.Flush()
	if err != nil {
		return err
	}

	return nil
}
