package donutdb

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/klauspost/compress/zstd"
)

func (f *file) getSector(sectorOffset int64) (*sector, error) {
	rangeKeyStr := strconv.FormatInt(sectorOffset, 10)

	out, err := f.vfs.db.Query(&dynamodb.QueryInput{
		TableName:              &f.vfs.table,
		KeyConditionExpression: aws.String("hash_key = :hk AND range_key = :rk"),
		ProjectionExpression:   aws.String("bytes"),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.dataRowKey,
			},
			":rk": {
				N: &rangeKeyStr,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(out.Items) == 0 {
		return nil, sectorNotFoundErr
	}

	item := out.Items[0]
	attr, ok := item["bytes"]
	if !ok {
		return nil, errors.New("no bytes attr found")
	}

	compressedSectorData := attr.B

	r, err := zstd.NewReader(bytes.NewReader(compressedSectorData))
	if err != nil {
		panic(err)
	}

	sectorData, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}

	r.Close()

	s := sector{
		offset: sectorOffset,
		data:   sectorData,
	}

	return &s, nil
}

func (f *file) getLastSector() (*sector, error) {
	out, err := f.vfs.db.Query(&dynamodb.QueryInput{
		TableName:              &f.vfs.table,
		KeyConditionExpression: aws.String("hash_key = :hk"),
		ProjectionExpression:   aws.String("range_key, bytes"),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int64(1),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk": {
				S: &f.dataRowKey,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(out.Items) == 0 {
		return nil, sectorNotFoundErr
	}

	item := out.Items[0]

	if item[rKey].N == nil {
		return nil, fmt.Errorf("range_key is not a number")
	}
	sectorOffset, err := strconv.ParseInt(*item[rKey].N, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[rKey].N, err)
	}

	compressedSectorData := item["bytes"].B

	r, err := zstd.NewReader(bytes.NewReader(compressedSectorData))
	if err != nil {
		panic(err)
	}

	sectorData, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}

	r.Close()

	return &sector{
		offset: sectorOffset,
		data:   sectorData,
	}, nil
}

func (f *file) getSectorRange(firstSector, lastSector int64) ([]sector, error) {
	startSector := firstSector
	endSector := lastSector

	if startSector == endSector {
		sect, err := f.getSector(firstSector)
		if err == sectorNotFoundErr {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return []sector{*sect}, nil
	}

	query := "hash_key = :hk AND range_key BETWEEN :first_sector AND :last_sector"
	var sectors []sector
	prevSectorOffset := firstSector - defaultSectorSize

	for {
		startSectorStr := strconv.FormatInt(startSector, 10)
		endSectorStr := strconv.FormatInt(endSector, 10)

		out, err := f.vfs.db.Query(&dynamodb.QueryInput{
			TableName:              &f.vfs.table,
			KeyConditionExpression: &query,
			ProjectionExpression:   aws.String("range_key, bytes"),
			Limit:                  aws.Int64(1000),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				":hk": {
					S: &f.dataRowKey,
				},
				":first_sector": {
					N: &startSectorStr,
				},
				":last_sector": {
					N: &endSectorStr,
				},
			},
		})

		if err != nil {
			return nil, err
		}

		for _, item := range out.Items {
			if item[rKey].N == nil {
				return nil, fmt.Errorf("range_key is not a number")
			}
			sectorOffset, err := strconv.ParseInt(*item[rKey].N, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("range_key does not parse to an int: %s %w", *item[rKey].N, err)
			}

			if sectorOffset != prevSectorOffset+defaultSectorSize {
				return nil, fmt.Errorf("Unexpected sector offset for range %d-%d, prev=%d got=%d expected=%d", firstSector, lastSector, prevSectorOffset, sectorOffset, prevSectorOffset+defaultSectorSize)
			}

			compressedSectorData := item["bytes"].B

			r, err := zstd.NewReader(bytes.NewReader(compressedSectorData))
			if err != nil {
				panic(err)
			}

			sectorData, err := ioutil.ReadAll(r)
			if err != nil {
				panic(err)
			}

			r.Close()

			sectors = append(sectors, sector{
				offset: sectorOffset,
				data:   sectorData,
			})
			prevSectorOffset = sectorOffset
		}

		if len(out.Items) == 0 {
			break
		}

		end := sectors[len(sectors)-1]
		if end.offset == lastSector {
			break
		}

		startSector = end.offset + 1
	}

	return sectors, nil
}

var sectorNotFoundErr = errors.New("sector not found")

type sector struct {
	offset int64
	data   []byte
}
