package donutdb

import "io"

type Option interface {
	setOption(*options) error
}

type options struct {
	sectorSize      int64
	changeLogWriter io.Writer
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
