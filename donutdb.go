package donutdb

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/sqlite3vfs"
)

const sectorSize = 4096

func New(dynamoClient *dynamodb.DynamoDB, table string) sqlite3vfs.VFS {
	ownerIDBytes := make([]byte, 8)
	rand.Read(ownerIDBytes)
	return &vfs{
		db:      dynamoClient,
		table:   table,
		ownerID: hex.EncodeToString(ownerIDBytes),
	}
}

type vfs struct {
	db      *dynamodb.DynamoDB
	table   string
	ownerID string
}

func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	f := file{
		name:    "fileV1-" + name,
		rawName: name,
		vfs:     v,

		lockManager: newGlobalLockManger(v.db, v.table, name, v.ownerID),
	}
	return &f, flags, nil
}

func (v *vfs) Delete(name string, dirSync bool) error {
	_, err := v.db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &v.table,
		Key:       dynamoKey(filesKey, filesRangeKey),
		AttributeUpdates: map[string]*dynamodb.AttributeValueUpdate{
			name: {
				Action: aws.String("DELETE"),
			},
		},
	})
	return err
}

var (
	filesKey      = "files"
	filesRangeKey = 0
)

func (v *vfs) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	got, err := v.db.GetItem(&dynamodb.GetItemInput{
		TableName:       &v.table,
		Key:             dynamoKey(filesKey, filesRangeKey),
		AttributesToGet: []*string{&name},
	})

	if err != nil {
		return false, err
	}

	_, exists := got.Item[name]

	if flag == sqlite3vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs *vfs) FullPathname(name string) string {
	return filepath.Clean(name)
}

type file struct {
	name    string
	rawName string
	closed  bool
	vfs     *vfs

	lockManager lockManager
}

func (f *file) Close() error {
	f.closed = true
	err := f.lockManager.close()
	if err != nil {
		return err
	}
	return nil
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	firstSector := sectorForPos(off)

	fileSize, err := f.FileSize()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := sectorForPos(lastByte)

	var sect sector
	iter := f.newSectorIterator(&sect, firstSector, lastSector)

	var (
		n     int
		first = true
	)
	for iter.Next() {
		if first {
			startIndex := off % sectorSize
			n = copy(p, sect.data[startIndex:])
			first = false
			continue
		}

		nn := copy(p[n:], sect.data)
		n += nn
	}
	err = iter.Close()
	if err != nil {
		return n, err
	}

	if lastByte >= fileSize {
		return n, io.EOF
	}

	return n, nil
}

func (f *file) WriteAt(b []byte, off int64) (n int, err error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	var writeCount int

	oldFileSize, err := f.FileSize()
	if err != nil {
		return writeCount, fmt.Errorf("filesize err: %w", err)
	}

	firstSector := sectorForPos(off)

	oldLastSector := sectorForPos(oldFileSize)

	secWriter := &sectorWriter{
		f: f,
	}

	for sectorStart := oldLastSector; sectorStart < firstSector; sectorStart += sectorSize {
		sectorLastBytePossible := sectorStart + sectorSize - 1
		if off > int64(sectorLastBytePossible) {
			if oldFileSize <= sectorStart {
				err = secWriter.writeSector(&sector{
					offset: sectorStart,
					data:   make([]byte, sectorSize),
				})
				if err != nil {
					return writeCount, fmt.Errorf("fill sector (off=%d) err: %w", sectorStart, err)
				}
				// create sector as empty
			} else if oldFileSize < int64(sectorLastBytePossible) {
				// fill existing sector
				sect, err := f.getSector(sectorStart)
				if err != nil {
					return 0, err
				}
				fill := make([]byte, sectorSize-len(sect.data))
				sect.data = append(sect.data, fill...)
				err = secWriter.writeSector(sect)
				if err != nil {
					return 0, err
				}
			} else {
				// this is a full sector, don't do anything
			}
		}
	}

	// we've hydrated all preceeding data
	lastSectorOffset := (off + int64(len(b))) - ((off + int64(len(b))) % sectorSize)

	var sect sector
	iter := f.newSectorIterator(&sect, firstSector, lastSectorOffset)

	// fill all but last sector
	var (
		idx      int
		iterDone bool
	)
	for sec := firstSector; sec <= lastSectorOffset; sec += sectorSize {

		if iterDone {
			sect = sector{
				offset: sec,
				data:   make([]byte, 0),
			}

		} else {
			if !iter.Next() {
				iterDone = true
				err := iter.Close()
				if err != nil {
					return writeCount, fmt.Errorf("get sector err: %w", err)
				}
				sect = sector{
					offset: sec,
					data:   make([]byte, 0),
				}
			}
		}

		var offsetIntoSector int64
		if idx == 0 {
			offsetIntoSector = off % sectorSize
		}

		if sect.offset < lastSectorOffset && len(sect.data) < sectorSize {
			fill := make([]byte, sectorSize-len(sect.data))
			sect.data = append(sect.data, fill...)
		} else if sect.offset == lastSectorOffset && len(sect.data) < int(offsetIntoSector)+len(b) {
			fill := make([]byte, int(offsetIntoSector)+len(b)-len(sect.data))
			sect.data = append(sect.data, fill...)
		}

		n := copy(sect.data[offsetIntoSector:], b)
		b = b[n:]

		sectCopy := sect
		err = secWriter.writeSector(&sectCopy)
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

	err = secWriter.flush()
	if err != nil {
		return 0, err
	}

	return writeCount, nil
}

func (f *file) Truncate(size int64) error {
	fileSize, err := f.FileSize()
	if err != nil {
		return err
	}

	if size >= fileSize {
		return nil
	}

	firstSector := sectorForPos(size)

	sect, err := f.getSector(firstSector)
	if err != nil {
		return err
	}

	sect.data = sect.data[:size%sectorSize]

	secWriter := &sectorWriter{
		f: f,
	}

	secWriter.writeSector(sect)

	lastSector := sectorForPos(fileSize)

	startSector := firstSector + sectorSize
	for sectToDelete := startSector; sectToDelete <= lastSector; sectToDelete += sectorSize {
		secWriter.deleteSector(sectToDelete)
	}

	return secWriter.flush()
}

func sectorForPos(pos int64) int64 {
	return pos - (pos % sectorSize)
}

func (f *file) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

func (f *file) FileSize() (int64, error) {
	sector, err := f.getLastSector()
	if err == sectorNotFoundErr {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	return sector.offset + int64(len(sector.data)), nil
}

func (f *file) Lock(elock sqlite3vfs.LockType) error {
	//    UNLOCKED -> SHARED
	//    SHARED -> RESERVED
	//    SHARED -> (PENDING) -> EXCLUSIVE
	//    RESERVED -> (PENDING) -> EXCLUSIVE
	//    PENDING -> EXCLUSIVE

	curLevel := f.lockManager.level()

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

	return f.lockManager.lock(elock)
}

func (f *file) Unlock(elock sqlite3vfs.LockType) error {
	return f.lockManager.unlock(elock)
}

func (f *file) CheckReservedLock() (bool, error) {
	return f.lockManager.checkReservedLock()
}

func (f *file) SectorSize() int64 {
	return sectorSize
}

func (f *file) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapAtomic4K | sqlite3vfs.IocapSafeAppend | sqlite3vfs.IocapSequential
}

var (
	hKey = "hash_key"
	rKey = "range_key"
)

func dynamoKey(hashKey string, rangeKey int) map[string]*dynamodb.AttributeValue {
	rangeKeyStr := strconv.Itoa(rangeKey)
	return map[string]*dynamodb.AttributeValue{
		hKey: {
			S: &hashKey,
		},
		rKey: {
			N: &rangeKeyStr,
		},
	}
}
