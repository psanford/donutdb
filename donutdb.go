package donutdb

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/sqlite3vfs"
)

const (
	defaultSectorSize = 1 << 17

	fileMetaKey    = "file-meta-v1"
	fileDataPrefix = "file-v1-"
	fileLockPrefix = "lock-global-v1-"
)

type Option struct {
}

func New(dynamoClient *dynamodb.DynamoDB, table string, opts ...Option) sqlite3vfs.VFS {
	ownerIDBytes := make([]byte, 8)
	if _, err := rand.Read(ownerIDBytes); err != nil {
		panic(err)
	}
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
	meta := fileMetaV1{
		MetaVersion: 1,
		OrigName:    name,
		SectorSize:  defaultSectorSize,
		CompressAlg: "zstd",
	}

	// try in loop incase we a racing with another client.
	// give up if we fail 100 times in a row
	for i := 0; i < 100; i++ {
		existing, err := v.db.GetItem(&dynamodb.GetItemInput{
			TableName:            &v.table,
			ConsistentRead:       aws.Bool(true),
			ProjectionExpression: aws.String("#fname"),
			ExpressionAttributeNames: map[string]*string{
				"#fname": aws.String(name),
			},
			Key: map[string]*dynamodb.AttributeValue{
				hKey: {
					S: aws.String("file-meta-v1"),
				},
				rKey: {
					N: aws.String("0"),
				},
			},
		})

		if err != nil {
			return nil, 0, err
		}

		if len(existing.Item) == 0 {
			fileIDBytes := make([]byte, 20)
			rand.Read(fileIDBytes)
			if _, err := rand.Read(fileIDBytes); err != nil {
				panic(err)
			}

			meta.RandID = base64.URLEncoding.EncodeToString(fileIDBytes)
			meta.DataRowKey = fileDataPrefix + meta.RandID + "-" + name
			meta.LockRowKey = fileLockPrefix + meta.RandID + "-" + name

			metaBytes, err := json.Marshal(meta)
			if err != nil {
				return nil, 0, err
			}

			_, err = v.db.UpdateItem(&dynamodb.UpdateItemInput{
				TableName:           &v.table,
				UpdateExpression:    aws.String("SET #fname=:meta"),
				ConditionExpression: aws.String("attribute_not_exists(#fname)"),
				Key: map[string]*dynamodb.AttributeValue{
					hKey: {
						S: aws.String("file-meta-v1"),
					},
					rKey: {
						N: aws.String("0"),
					},
				},
				ExpressionAttributeNames: map[string]*string{
					"#fname": aws.String(name),
				},
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":meta": {
						S: aws.String(string(metaBytes)),
					},
				},
			})

			if err != nil {
				if _, match := err.(*dynamodb.ConditionalCheckFailedException); match {
					// we raced with another client, retry
					continue
				}
				return nil, 0, err
			}

			f := v.fileFromMeta(&meta)
			return f, flags, nil
		} else {
			err = json.Unmarshal([]byte(*existing.Item[name].S), &meta)
			if err != nil {
				return nil, 0, fmt.Errorf("decode file metadata err: %w", err)
			}

			f := v.fileFromMeta(&meta)
			return f, flags, nil
		}
	}

	return nil, flags, errors.New("failed to get/create file metadata too many times due to races")
}

func (v *vfs) Delete(name string, dirSync bool) error {
	existing, err := v.db.Query(&dynamodb.QueryInput{
		TableName:            &v.table,
		Limit:                aws.Int64(1),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#fname"),
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: aws.String("file-meta-v1"),
			},
			":rk": {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		return err
	}

	if len(existing.Items) == 0 {
		return nil
	}

	metaBytes := *existing.Items[0][name].S

	var meta fileMetaV1
	err = json.Unmarshal([]byte(metaBytes), &meta)
	if err != nil {
		return fmt.Errorf("unmarshal file meta v1 err: %w", err)
	}

	_, err = v.db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:           &v.table,
		UpdateExpression:    aws.String("REMOVE #fname"),
		ConditionExpression: aws.String("#fname=:meta"),
		Key: map[string]*dynamodb.AttributeValue{
			hKey: {
				S: aws.String("file-meta-v1"),
			},
			rKey: {
				N: aws.String("0"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":meta": {
				S: aws.String(metaBytes),
			},
		},
	})

	if err != nil {
		return err
	}

	f := v.fileFromMeta(&meta)

	lastSec, err := f.getLastSector()
	if err != nil {
		return err
	}

	secWriter := &sectorWriter{
		f: f,
	}

	for sectToDelete := lastSec.offset; sectToDelete >= 0; sectToDelete -= f.sectorSize {
		secWriter.deleteSector(sectToDelete)
	}

	err = secWriter.flush()
	if err != nil {
		return err
	}
	return nil
}

func (v *vfs) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	existing, err := v.db.Query(&dynamodb.QueryInput{
		TableName:            &v.table,
		Limit:                aws.Int64(1),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("#fname"),
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(name),
		},
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: aws.String("file-meta-v1"),
			},
			":rk": {
				N: aws.String("0"),
			},
		},
	})

	if err != nil {
		return false, err
	}

	exists := len(existing.Items) > 0 && len(existing.Items[0]) > 0

	if flag == sqlite3vfs.AccessExists {
		return exists, nil
	}

	return true, nil
}

func (vfs *vfs) FullPathname(name string) string {
	name = filepath.Clean(string(filepath.Separator) + name)
	return name
}

type file struct {
	dataRowKey string
	rawName    string
	randID     string
	sectorSize int64
	closed     bool
	vfs        *vfs

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
		return 0, os.ErrClosed
	}

	firstSector := f.sectorForPos(off)

	fileSize, err := f.FileSize()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := f.sectorForPos(lastByte)

	var sect sector
	iter := f.newSectorIterator(&sect, firstSector, lastSector)

	var (
		n     int
		first = true
	)
	for iter.Next() {
		if first {
			startIndex := off % f.sectorSize
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
		return 0, os.ErrClosed
	}

	var writeCount int

	oldFileSize, err := f.FileSize()
	if err != nil {
		return writeCount, fmt.Errorf("filesize err: %w", err)
	}

	firstSector := f.sectorForPos(off)

	oldLastSector := f.sectorForPos(oldFileSize)

	secWriter := &sectorWriter{
		f: f,
	}

	for sectorStart := oldLastSector; sectorStart < firstSector; sectorStart += f.sectorSize {
		sectorLastBytePossible := sectorStart + f.sectorSize - 1
		if off > int64(sectorLastBytePossible) {
			if oldFileSize <= sectorStart {
				err = secWriter.writeSector(&sector{
					offset: sectorStart,
					data:   make([]byte, f.sectorSize),
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
				fill := make([]byte, f.sectorSize-int64(len(sect.data)))
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

	err = secWriter.flush()
	if err != nil {
		return 0, err
	}

	// we've hydrated all preceeding data
	lastSectorOffset := (off + int64(len(b))) - ((off + int64(len(b))) % f.sectorSize)

	var sect sector
	iter := f.newSectorIterator(&sect, firstSector, lastSectorOffset)

	// fill all but last sector
	var (
		idx      int
		iterDone bool
	)
	for sec := firstSector; sec <= lastSectorOffset; sec += f.sectorSize {

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
			offsetIntoSector = off % f.sectorSize
		}

		if sect.offset < lastSectorOffset && int64(len(sect.data)) < f.sectorSize {
			fill := make([]byte, f.sectorSize-int64(len(sect.data)))
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

	firstSector := f.sectorForPos(size)

	sect, err := f.getSector(firstSector)
	if err != nil {
		return err
	}

	sect.data = sect.data[:size%f.sectorSize]

	secWriter := &sectorWriter{
		f: f,
	}

	secWriter.writeSector(sect)

	lastSector := f.sectorForPos(fileSize)

	startSector := firstSector + f.sectorSize
	for sectToDelete := startSector; sectToDelete <= lastSector; sectToDelete += f.sectorSize {
		secWriter.deleteSector(sectToDelete)
	}

	return secWriter.flush()
}

func (f *file) sectorForPos(pos int64) int64 {
	return pos - (pos % f.sectorSize)
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
	return f.sectorSize
}

func (f *file) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapAtomic64K | sqlite3vfs.IocapSafeAppend | sqlite3vfs.IocapSequential
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

type fileMetaV1 struct {
	MetaVersion int    `json:"meta_version"`
	SectorSize  int64  `json:"sector_size"`
	OrigName    string `json:"orig_name"`
	RandID      string `json:"rand_id"`
	DataRowKey  string `json:"data_row_key"`
	LockRowKey  string `json:"lock_row_key"`
	CompressAlg string `json:"compress_alg"`
}

func (v *vfs) fileFromMeta(meta *fileMetaV1) *file {
	return &file{
		dataRowKey: fileDataPrefix + meta.RandID + "-" + meta.OrigName,
		rawName:    meta.OrigName,
		randID:     meta.RandID,
		sectorSize: meta.SectorSize,
		vfs:        v,

		lockManager: newGlobalLockManger(v.db, v.table, meta.LockRowKey, v.ownerID),
	}
}
