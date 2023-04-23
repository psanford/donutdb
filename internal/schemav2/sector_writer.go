package schemav2

import (
	"crypto/sha512"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
	"github.com/psanford/donutdb/internal/dynamo"
	"golang.org/x/exp/maps"
)

// SectorWriter is a buffered writer for sectors.
// You must call flush() and check its error to ensure
// all sectors are actually written
type SectorWriter struct {
	F    *File
	meta *dynamo.FileMetaV1V2
	err  error

	skipMetadataUpdates  bool
	pendingWriteSectors  map[int]Sector
	pendingDeleteSectors []string
}

var encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))

var compressFunc = func(data []byte) []byte {
	compBytes := make([]byte, 0, len(data))
	return encoder.EncodeAll(data, compBytes)
}

// var compressFunc = func(data []byte) []byte {
// 	return data
// }

func (w *SectorWriter) WriteSector(idx int, data []byte) error {
	if w.err != nil {
		return w.err
	}

	h := sha512.New512_256()
	h.Write(data)
	sum := h.Sum(nil)

	id := fmt.Sprintf("%d__%x", idx, sum)
	s := &Sector{
		Data: data,
		ID:   id,
		idx:  idx,
	}

	if idx < len(w.meta.Sectors) {
		if w.meta.Sectors[idx] == id {
			return nil
		}
	}

	if w.pendingWriteSectors == nil {
		w.pendingWriteSectors = make(map[int]Sector)
	}

	w.pendingWriteSectors[idx] = *s

	endPos := (int64(idx) * w.F.sectorSize) + int64(len(data))
	if endPos > w.meta.FileSize {
		w.meta.FileSize = endPos
	}

	if len(w.meta.Sectors) <= idx {
		w.meta.Sectors = append(w.meta.Sectors, make([]string, idx-len(w.meta.Sectors)+1)...)
	}
	w.meta.Sectors[idx] = s.ID

	if len(w.pendingWriteSectors)+len(w.pendingDeleteSectors) == 25 {
		return w.Flush()
	}

	return nil
}

func (w *SectorWriter) DeleteSector(id string) error {
	if w.err != nil {
		return w.err
	}

	w.pendingDeleteSectors = append(w.pendingDeleteSectors, id)

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
		w.F.sectcache.Put(s.ID, s.Data)

		compBytes := compressFunc(s.Data)

		key := "file-v2-" + w.F.randID + "-" + w.F.rawName + "-" + s.ID

		req := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					dynamo.HKey: {
						S: &key,
					},
					dynamo.RKey: {
						N: aws.String("0"),
					},
					"bytes": {
						B: compBytes,
					},
				},
			},
		}
		reqs = append(reqs, req)
	}

	for _, id := range w.pendingDeleteSectors {
		key := "file-v2-" + w.F.randID + "-" + w.F.rawName + "-" + id
		req := &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					dynamo.HKey: {
						S: &key,
					},
					dynamo.RKey: {
						N: aws.String("0"),
					},
				},
			},
		}
		reqs = append(reqs, req)
	}

	items := map[string][]*dynamodb.WriteRequest{
		w.F.table: reqs,
	}

	t0 := time.Now()
	resp, err := w.F.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})
	if err != nil {
		w.err = err
		return err
	}

	if len(resp.UnprocessedItems) > 0 {
		w.err = fmt.Errorf("unprocessed items: %v", resp.UnprocessedItems)
	}

	BatchWriteItemHist.Observe(time.Since(t0).Seconds())
	BatchWriteItemCount.Add(float64(len(items)))

	maps.Clear(w.pendingWriteSectors)
	w.pendingDeleteSectors = w.pendingDeleteSectors[:0]

	if !w.skipMetadataUpdates {
		w.err = w.F.updateMeta(w.meta)
	}

	return w.err
}
