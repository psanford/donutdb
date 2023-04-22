package donutdb

import (
	"errors"
	"io"

	"github.com/psanford/donutdb/sectorcache"
)

type Option interface {
	setOption(*options) error
}

type options struct {
	sectorSize           int64
	changeLogWriter      io.Writer
	defaultSchemaVersion int
	sectorCache          sectorcache.CacheV2
}

type sectorSizeOption struct {
	sectorSize int64
}

func (o sectorSizeOption) setOption(opts *options) error {
	opts.sectorSize = o.sectorSize
	return nil
}

func WithSectorSize(s int64) Option {
	return sectorSizeOption{
		sectorSize: s,
	}
}

type changeLogOption struct {
	changeLogWriter io.Writer
}

func (o changeLogOption) setOption(opts *options) error {
	opts.changeLogWriter = o.changeLogWriter
	return nil
}

func WithChangeLogWriter(w io.Writer) Option {
	return &changeLogOption{
		changeLogWriter: w,
	}
}

type defaultSchemaVersionOption struct {
	version int
}

func (o defaultSchemaVersionOption) setOption(opts *options) error {
	if o.version < 0 || o.version > 2 {
		return errors.New("unknown schema version specified")
	}
	opts.defaultSchemaVersion = o.version
	return nil
}

func WithDefaultSchemaVersion(v int) Option {
	return &defaultSchemaVersionOption{
		version: v,
	}
}

type sectorCacheOption struct {
	sectorCache sectorcache.CacheV2
}

func (o sectorCacheOption) setOption(opts *options) error {
	opts.sectorCache = o.sectorCache
	return nil
}

// WithSectorCacheV2 sets an optional cache strategy for
// schemav2 files.
func WithSectorCacheV2(c sectorcache.CacheV2) Option {
	return &sectorCacheOption{
		sectorCache: c,
	}
}
