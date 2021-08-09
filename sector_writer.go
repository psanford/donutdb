package donutdb

import (
	"strconv"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// sectorWriter is a buffered writer for sectors.
// You must call flush() and check its error to ensure
// all sectors are actually written
type sectorWriter struct {
	f   *file
	err error

	pendingSectors []sector
}

func (w *sectorWriter) writeSector(s *sector) error {
	if w.err != nil {
		return w.err
	}

	w.pendingSectors = append(w.pendingSectors, *s)

	if len(w.pendingSectors) == 25 {
		return w.flush()
	}

	return nil
}

func (w *sectorWriter) flush() error {
	if w.err != nil {
		return w.err
	}

	if len(w.pendingSectors) == 0 {
		return nil
	}

	reqs := make([]*dynamodb.WriteRequest, len(w.pendingSectors))

	for i, s := range w.pendingSectors {
		rangeKeyStr := strconv.FormatInt(s.offset, 10)

		reqs[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					hKey: {
						S: &w.f.name,
					},
					rKey: {
						N: &rangeKeyStr,
					},
					"bytes": {
						B: s.data,
					},
				},
			},
		}
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

	w.pendingSectors = w.pendingSectors[:0]
	return nil
}
