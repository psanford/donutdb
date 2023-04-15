package donutdb

import (
	"errors"
)

type sectorIterator struct {
	f                *file
	lastSectorOffset int64
	sectorSize       int64
	sectorPtr        *sector

	offset        int64
	cachedSectors []sector
	err           error
}

func (f *file) newSectorIterator(sectorPtr *sector, firstSector, lastSectorOffset, sectorSize int64) *sectorIterator {
	return &sectorIterator{
		f:                f,
		lastSectorOffset: lastSectorOffset,
		sectorPtr:        sectorPtr,
		offset:           firstSector,
		sectorSize:       sectorSize,
	}
}

func (i *sectorIterator) Next() bool {
	if i.err != nil {
		return false
	}

	if i.offset > i.lastSectorOffset {
		return false
	}

	if len(i.cachedSectors) == 0 {
		sectors, err := i.f.getSectorRange(i.offset, i.lastSectorOffset)
		if err != nil {
			i.err = err
			return false
		}

		if len(sectors) == 0 {
			if i.offset < i.lastSectorOffset {
				i.err = sectorNotFoundErr
			}
			return false
		}

		i.cachedSectors = sectors
	}

	*i.sectorPtr = i.cachedSectors[0]
	i.cachedSectors = i.cachedSectors[1:]

	i.offset = i.sectorPtr.offset + i.sectorSize

	return true
}

func (i *sectorIterator) Close() error {
	if i.err != nil {
		return i.err
	}

	i.err = errors.New("iter closed")
	return nil
}
