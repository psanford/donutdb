package donutdb

import (
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
	return &vfs{
		db:    dynamoClient,
		table: table,
	}
}

type vfs struct {
	db    *dynamodb.DynamoDB
	table string
}

func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	f := file{
		name: name,
		vfs:  v,
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
	name   string
	closed bool
	vfs    *vfs
}

func (f *file) Close() error {
	f.closed = true
	return nil
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}

	firstSector := off - (off % sectorSize)

	fileSize, err := f.FileSize()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := lastByte - (lastByte % sectorSize)

	sectors, err := getSectorRange(f.vfs.db, f.vfs.table, f.name, firstSector, lastSector)
	if err != nil {
		return 0, err
	}

	var n int
	for i, sector := range sectors {
		if i == 0 {
			startIndex := off % sectorSize
			n = copy(p, sector.data[startIndex:])
			continue
		}

		nn := copy(p[n:], sector.data)
		n += nn
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

	firstSector := off - (off % sectorSize)

	oldLastSector := oldFileSize - (oldFileSize % sectorSize)

	for sectorStart := oldLastSector; sectorStart < firstSector; sectorStart += sectorSize {
		sectorLastBytePossible := sectorStart + sectorSize - 1
		if off > int64(sectorLastBytePossible) {
			if oldFileSize <= sectorStart {
				err = f.writeSector(&sector{
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
				err = f.writeSector(sect)
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

	// fill all but last sector
	var idx int
	for sec := firstSector; sec <= lastSectorOffset; sec += sectorSize {

		sect, err := f.getSector(sec)
		if err == sectorNotFoundErr {
			if sec%sectorSize != 0 {
				panic(fmt.Sprintf("sec not a modulo of sectorSize %d", sec))
			}
			sect = &sector{
				offset: sec,
				data:   make([]byte, 0),
			}
		} else if err != nil {
			return writeCount, fmt.Errorf("get sector err: %w", err)
		}

		var offsetIntoSector int64
		if idx == 0 {
			offsetIntoSector = off % sectorSize
		}

		if sec < lastSectorOffset && len(sect.data) < sectorSize {
			fill := make([]byte, sectorSize-len(sect.data))
			sect.data = append(sect.data, fill...)
		} else if sec == lastSectorOffset && len(sect.data) < int(offsetIntoSector)+len(b) {
			fill := make([]byte, int(offsetIntoSector)+len(b)-len(sect.data))
			sect.data = append(sect.data, fill...)
		}

		n := copy(sect.data[offsetIntoSector:], b)
		b = b[n:]

		err = f.writeSector(sect)
		if err != nil {
			return writeCount, fmt.Errorf("write sector err: %w", err)
		}
		writeCount += n
		idx++
	}

	return writeCount, nil
}

func (f *file) Truncate(size int64) error {
	panic("truncate not implemented")
	return sqlite3vfs.ReadOnlyError
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
	return nil
}

func (f *file) Unlock(elock sqlite3vfs.LockType) error {
	return nil
}

func (f *file) CheckReservedLock() (bool, error) {
	return false, nil
}

func (f *file) SectorSize() int64 {
	return sectorSize
}

func (f *file) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return 0
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
