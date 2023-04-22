package schemav2

import (
	"crypto/sha512"
	"fmt"

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

func (w *SectorWriter) WriteSector(idx int, data []byte) error {
	if w.err != nil {
		return w.err
	}

	h := sha512.New512_256()
	h.Write([]byte(fmt.Sprintf("%d-", idx)))
	h.Write(data)
	sum := h.Sum(nil)

	id := fmt.Sprintf("%x", sum)
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

		if len(w.meta.Sectors) <= s.idx {
			w.meta.Sectors = append(w.meta.Sectors, make([]string, s.idx-len(w.meta.Sectors)+1)...)
		}
		w.meta.Sectors[s.idx] = s.ID
		endPos := (int64(s.idx) * w.F.sectorSize) + int64(len(s.Data))
		if endPos > w.meta.FileSize {
			w.meta.FileSize = endPos
		}
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

	_, err := w.F.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})

	if err != nil {
		w.err = err
		return err
	}

	maps.Clear(w.pendingWriteSectors)
	w.pendingDeleteSectors = w.pendingDeleteSectors[:0]

	if !w.skipMetadataUpdates {
		err = w.F.updateMeta(w.meta)
	}

	return nil
}
