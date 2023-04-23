package schemav1

import (
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
	"github.com/psanford/donutdb/internal/dynamo"
)

// SectorWriter is a buffered writer for sectors.
// You must call flush() and check its error to ensure
// all sectors are actually written
type SectorWriter struct {
	F   *File
	err error

	pendingWriteSectors  []Sector
	pendingDeleteSectors []int64
}

var encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))

func (w *SectorWriter) WriteSector(s *Sector) error {
	if w.err != nil {
		return w.err
	}

	w.pendingWriteSectors = append(w.pendingWriteSectors, *s)

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 25 {
		return w.Flush()
	}

	return nil
}

func (w *SectorWriter) DeleteSector(s int64) error {
	if w.err != nil {
		return w.err
	}

	w.pendingDeleteSectors = append(w.pendingDeleteSectors, s)

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 25 {
		return w.Flush()
	}

	return nil
}

func (w *SectorWriter) Flush() error {
	if w.err != nil {
		return w.err
	}

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 0 {
		return nil
	}

	reqs := make([]*dynamodb.WriteRequest, 0, len(w.pendingWriteSectors)+len(w.pendingDeleteSectors))

	for _, s := range w.pendingWriteSectors {
		rangeKeyStr := strconv.FormatInt(s.Offset, 10)

		compBytes := make([]byte, 0, len(s.Data))
		compBytes = encoder.EncodeAll(s.Data, compBytes)

		req := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					dynamo.HKey: {
						S: &w.F.dataRowKey,
					},
					dynamo.RKey: {
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
					dynamo.HKey: {
						S: &w.F.dataRowKey,
					},
					dynamo.RKey: {
						N: &rangeKeyStr,
					},
				},
			},
		}
		reqs = append(reqs, req)
	}

	items := map[string][]*dynamodb.WriteRequest{
		w.F.table: reqs,
	}

	resp, err := w.F.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})

	if err != nil {
		w.err = err
		return err
	}

	if len(resp.UnprocessedItems) > 0 {
		// we should retry these, but until we do we need to error
		w.err = fmt.Errorf("unprocessed items: %v", resp.UnprocessedItems)
		return w.err
	}

	w.pendingWriteSectors = w.pendingWriteSectors[:0]
	w.pendingDeleteSectors = w.pendingDeleteSectors[:0]
	return nil
}
