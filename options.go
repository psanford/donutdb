package donutdb

type Option interface {
	setOption(*options) error
}

type options struct {
	sectorSize int64
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
