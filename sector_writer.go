package donutdb

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
)

// sectorWriter is a buffered writer for sectors.
// You must call flush() and check its error to ensure
// all sectors are actually written
type sectorWriter struct {
	f   *file
	err error

	pendingWriteSectors  []sector
	pendingDeleteSectors []int64
}

var encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))

func (w *sectorWriter) writeSector(s *sector) error {
	if w.err != nil {
		return w.err
	}

	w.pendingWriteSectors = append(w.pendingWriteSectors, *s)

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 25 {
		return w.flush()
	}

	return nil
}

func (w *sectorWriter) deleteSector(s int64) error {
	if w.err != nil {
		return w.err
	}

	w.pendingDeleteSectors = append(w.pendingDeleteSectors, s)

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 25 {
		return w.flush()
	}

	return nil
}

func (w *sectorWriter) flush() error {
	if w.err != nil {
		return w.err
	}

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 0 {
		return nil
	}

	reqs := make([]*dynamodb.WriteRequest, 0, len(w.pendingWriteSectors)+len(w.pendingDeleteSectors))

	for _, s := range w.pendingWriteSectors {
		rangeKeyStr := strconv.FormatInt(s.offset, 10)

		compBytes := make([]byte, 0, len(s.data))
		compBytes = encoder.EncodeAll(s.data, compBytes)

		req := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					hKey: {
						S: &w.f.dataRowKey,
					},
					rKey: {
						N: &rangeKeyStr,
					},
					"bytes": {
						B: compBytes,
					},
				},
			},
		}
		reqs = append(reqs, req)
	}

	for _, s := range w.pendingDeleteSectors {
		rangeKeyStr := strconv.FormatInt(s, 10)
		req := &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					hKey: {
						S: &w.f.dataRowKey,
					},
					rKey: {
						N: &rangeKeyStr,
					},
				},
			},
		}
		reqs = append(reqs, req)
	}

	items := map[string][]*dynamodb.WriteRequest{
		w.f.vfs.table: reqs,
	}

	_, err := w.f.vfs.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})

	if err != nil {
		w.err = err
		return err
	}

	w.pendingWriteSectors = w.pendingWriteSectors[:0]
	w.pendingDeleteSectors = w.pendingDeleteSectors[:0]
	return nil
}
