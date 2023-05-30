package schemav2

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/psanford/donutdb/internal/changelog"
	"github.com/psanford/donutdb/internal/dynamo"
	"github.com/psanford/donutdb/internal/lock"
	"github.com/psanford/donutdb/sectorcache"
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
	sectcache       sectorcache.CacheV2

	sectorWriter *SectorWriter

	cachedSize int64

	lockManager lock.LockManager
}

func FileFromMeta(meta *dynamo.FileMetaV1V2, table, ownerID string, db *dynamodb.DynamoDB, changeLogWriter *json.Encoder, cache sectorcache.CacheV2) (*File, error) {
	if meta.MetaVersion != 2 {
		return nil, fmt.Errorf("cannot instanciate schemav2 file for MetaVersion=%d", meta.MetaVersion)
	}

	if cache == nil {
		cache = &nopCache{}
	}

	f := File{
		dataRowKey:      dynamo.FileDataPrefix + meta.RandID + "-" + meta.OrigName,
		rawName:         meta.OrigName,
		randID:          meta.RandID,
		sectorSize:      meta.SectorSize,
		table:           table,
		db:              db,
		changeLogWriter: changeLogWriter,
		sectcache:       cache,

		lockManager: lock.NewGlobalLockManger(db, table, meta.LockRowKey, ownerID),
	}

	return &f, nil
}

func (f *File) Close() error {
	f.closed = true

	f.Sync(sqlite3vfs.SyncNormal)

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

	if f.sectorWriter != nil {
		err := f.sectorWriter.Flush()
		if err != nil {
			return 0, err
		}
		f.sectorWriter = nil
	}

	firstSectorIdx := f.sectorIdxForPos(off)

	meta, err := f.currentMeta()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSectorIdx := f.sectorIdxForPos(lastByte) + 1

	if firstSectorIdx >= len(meta.Sectors) {
		return 0, io.EOF
	}

	if lastSectorIdx > len(meta.Sectors) {
		lastSectorIdx = len(meta.Sectors)
	}

	sectors := meta.Sectors[firstSectorIdx:lastSectorIdx]

	var sect Sector
	iter := f.newSectorIterator(&sect, sectors)

	var (
		n            int
		first        = true
		iterCount    int
		prevSeenSize = f.sectorSize
	)

	for iter.Next() {
		if prevSeenSize != f.sectorSize {
			return n, fmt.Errorf("non-full sector detected in the middle of a file idx=%d size=%d", firstSectorIdx+iterCount-1, prevSeenSize)
		}
		prevSeenSize = int64(len(sect.Data))
		if first {
			startIndex := off % f.sectorSize
			n = copy(p, sect.Data[startIndex:])
			first = false
			iterCount++
			continue
		}

		nn := copy(p[n:], sect.Data)
		n += nn
		iterCount++
	}
	err = iter.Close()
	if err == dynamo.SectorNotFoundErr && lastByte >= meta.FileSize {
		return n, io.EOF
	} else if err != nil {
		retErr = err
		return n, retErr
	}

	if lastByte >= meta.FileSize {
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

	meta, err := f.currentMeta()
	if err != nil {
		return 0, err
	}

	oldFileSize := meta.FileSize

	firstSectorIdx := f.sectorIdxForPos(off)

	defer func() {
		if off+int64(len(b)) > oldFileSize {
			f.cachedSize = off + int64(len(b))
		}
	}()

	if f.sectorWriter == nil {
		f.sectorWriter = &SectorWriter{
			F:    f,
			meta: meta,
		}
	}

	if firstSectorIdx >= len(meta.Sectors) {
		if meta.FileSize%f.sectorSize != 0 {
			// the last sector is not full, we need to fetch it and append to it
			idx := len(f.sectorWriter.meta.Sectors) - 1

			var sector Sector
			if pendingSector, found := f.sectorWriter.pendingWriteSectors[idx]; found {
				sector = pendingSector
			} else {
				lastSectorID := f.sectorWriter.meta.Sectors[idx]
				sectors, err := f.getSectors([]string{lastSectorID})
				if err != nil {
					return writeCount, err
				}

				sector = sectors[0]
			}

			additionalCount := int(f.sectorSize) - len(sector.Data)
			if additionalCount > 0 {
				sector.Data = append(sector.Data, make([]byte, additionalCount)...)
				f.sectorWriter.WriteSector(idx, sector.Data)
			}
		}

		for idx := len(meta.Sectors); idx < firstSectorIdx; idx++ {
			// create sector as empty
			data := make([]byte, f.sectorSize)
			err = f.sectorWriter.WriteSector(idx, data)
			if err != nil {
				return writeCount, fmt.Errorf("fill sector (off=%d) err: %w", int64(idx)*f.sectorSize, err)
			}
		}

	}

	posInFile := off

	for len(b) > 0 {
		dataForSector := int(f.sectorSize)
		if posInFile%f.sectorSize != 0 {
			dataForSector = int(f.sectorSize - (posInFile % f.sectorSize))
		}
		if len(b) < dataForSector {
			dataForSector = len(b)
		}

		curSectorData := b[:dataForSector]
		b = b[dataForSector:]

		offsetIntoSector := int(posInFile % f.sectorSize)

		curSectorIdx := f.sectorIdxForPos(posInFile)

		if len(curSectorData) != int(f.sectorSize) {
			// this is a partial sector, we need to fetch the existing sector data
			// to merge with the new data
			if pendingSector, found := f.sectorWriter.pendingWriteSectors[curSectorIdx]; found {
				existingData := pendingSector.Data
				if len(existingData) < offsetIntoSector+len(curSectorData) {
					existingData = append(existingData, make([]byte, offsetIntoSector+len(curSectorData)-len(existingData))...)
				}

				copy(existingData[offsetIntoSector:], curSectorData)
				f.sectorWriter.WriteSector(curSectorIdx, existingData)
			} else if curSectorIdx >= len(f.sectorWriter.meta.Sectors) {
				// this is a new sector
				existingData := make([]byte, offsetIntoSector+len(curSectorData))
				copy(existingData[offsetIntoSector:], curSectorData)
				f.sectorWriter.WriteSector(curSectorIdx, existingData)
			} else {
				sector, err := f.getSectors([]string{f.sectorWriter.meta.Sectors[curSectorIdx]})
				if err != nil {
					return writeCount, fmt.Errorf("get sector err: %w", err)
				}
				existingData := sector[0].Data
				if len(existingData) < offsetIntoSector+len(curSectorData) {
					existingData = append(existingData, make([]byte, offsetIntoSector+len(curSectorData)-len(existingData))...)
				}
				copy(existingData[offsetIntoSector:], curSectorData)
				f.sectorWriter.WriteSector(curSectorIdx, existingData)
			}
		} else {
			copiedData := make([]byte, len(curSectorData))
			copy(copiedData, curSectorData)
			f.sectorWriter.WriteSector(curSectorIdx, copiedData)
		}

		writeCount += dataForSector
		posInFile += int64(dataForSector)
		curSectorIdx++
	}

	// err = f.sectorWriter.Flush()
	// if err != nil {
	// 	return 0, err
	// }

	return writeCount, nil
}

func (f *File) SanityCheckSectors() error {
	meta, err := f.currentMeta()
	if err != nil {
		return err
	}

	var sect Sector
	iter := f.newSectorIterator(&sect, meta.Sectors)

	var seen int64
	for iter.Next() {
		seen = seen + int64(len(sect.Data))
	}

	err = iter.Close()
	if err != nil {
		return err
	}

	if seen != meta.FileSize {
		return fmt.Errorf("file size mismatch: %d != %d", seen, meta.FileSize)
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

	meta, err := f.currentMeta()
	if err != nil {
		return err
	}

	if size >= meta.FileSize {
		return nil
	}

	if f.sectorWriter == nil {
		f.sectorWriter = &SectorWriter{
			F:    f,
			meta: meta,
		}
	}
	firstSectorIdx := f.sectorIdxForPos(size)

	firstSectorIdxToDelete := firstSectorIdx

	if size%f.sectorSize != 0 {
		firstSectorIdxToDelete++
		sectors, err := f.getSectors([]string{meta.Sectors[firstSectorIdx]})
		if err != nil {
			return err
		}

		sectors[0].Data = sectors[0].Data[:size%f.sectorSize]

		f.sectorWriter.WriteSector(firstSectorIdx, sectors[0].Data)
	}

	lastSectorIdx := len(meta.Sectors) - 1

	for sectToDelete := firstSectorIdxToDelete; sectToDelete <= lastSectorIdx; sectToDelete++ {
		f.sectorWriter.DeleteSector(meta.Sectors[sectToDelete])
	}

	// err = f.sectorWriter.Flush()
	// if err != nil {
	// 	return err
	// }

	meta.FileSize = size
	// return f.updateMeta(meta)
	return nil
}

// sectorForPos returns the array index into the metadata
// sectors slice
func (f *File) sectorIdxForPos(pos int64) int {
	return int(pos / f.sectorSize)
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

	if f.sectorWriter != nil {
		err := f.sectorWriter.Flush()
		if err != nil {
			return err
		}
		f.sectorWriter = nil
	}

	return nil
}

func (f *File) currentMeta() (*dynamo.FileMetaV1V2, error) {
	if f.sectorWriter != nil {
		return f.sectorWriter.meta, nil
	}

	t0 := time.Now()
	existing, err := f.db.GetItem(&dynamodb.GetItemInput{
		TableName:            &f.table,
		ProjectionExpression: aws.String("#fname"),
		ExpressionAttributeNames: map[string]*string{
			"#fname": aws.String(f.rawName),
		},
		Key: map[string]*dynamodb.AttributeValue{
			dynamo.HKey: {
				S: aws.String(dynamo.FileMetaKey),
			},
			dynamo.RKey: {
				N: aws.String("0"),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	GetItemHist.Observe(float64(time.Since(t0).Seconds()))

	var meta dynamo.FileMetaV1V2
	err = json.Unmarshal([]byte(*existing.Item[f.rawName].S), &meta)
	if err != nil {
		return nil, fmt.Errorf("decode file metadata err: %w", err)
	}

	return &meta, nil
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

	meta, err := f.currentMeta()
	if err != nil {
		return 0, err
	}

	size := meta.FileSize

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

func (f *File) updateMeta(meta *dynamo.FileMetaV1V2) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	t0 := time.Now()
	_, err = f.db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:        &f.table,
		UpdateExpression: aws.String("SET #fname=:meta"),
		Key: map[string]*dynamodb.AttributeValue{
			dynamo.HKey: {
				S: aws.String(dynamo.FileMetaKey),
			},
			dynamo.RKey: {
				N: aws.String("0"),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#fname": &meta.OrigName,
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":meta": {
				S: aws.String(string(metaBytes)),
			},
		},
	})

	UpdateItemHist.Observe(time.Since(t0).Seconds())

	return err
}

func (f *File) CleanupSectors(meta *dynamo.FileMetaV1V2) error {
	secWriter := &SectorWriter{
		F:                   f,
		meta:                meta,
		skipMetadataUpdates: true,
	}

	for _, sect := range meta.Sectors {
		secWriter.DeleteSector(sect)
	}

	return secWriter.Flush()
}
