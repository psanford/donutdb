package schemav2

import (
	"errors"
	"fmt"
)

type sectorIterator struct {
	f              *File
	sectorsToFetch []string
	sectorPtr      *Sector

	cachedSectors []Sector
	err           error
}

type Sector struct {
	Valid bool
	Data  []byte
	ID    string
	idx   int
}

func (f *File) newSectorIterator(sectorPtr *Sector, sectors []string) *sectorIterator {
	return &sectorIterator{
		f:              f,
		sectorsToFetch: sectors,
		sectorPtr:      sectorPtr,
	}
}

func (i *sectorIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if len(i.cachedSectors) == 0 && len(i.sectorsToFetch) == 0 {
		return false
	}

	if len(i.cachedSectors) == 0 {
		maxSectors := 100
		if len(i.sectorsToFetch) < maxSectors {
			maxSectors = len(i.sectorsToFetch)
		}

		sectorIDs := i.sectorsToFetch[:maxSectors]
		i.sectorsToFetch = i.sectorsToFetch[maxSectors:]

		sectors, err := i.f.getSectors(sectorIDs)
		if err != nil {
			i.err = err
			return false
		}

		i.cachedSectors = sectors
	}

	if !i.cachedSectors[0].Valid {
		i.err = fmt.Errorf("sector %s was not found", i.cachedSectors[0].ID)
		return false
	}

	*i.sectorPtr = i.cachedSectors[0]
	i.cachedSectors = i.cachedSectors[1:]

	return true
}

func (i *sectorIterator) Close() error {
	if i.err != nil {
		return i.err
	}

	i.err = errors.New("iter closed")
	return nil
}
