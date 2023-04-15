package dynamo

import "errors"

const (
	DefaultSectorSize = 1 << 16

	FileMetaKey    = "file-meta-v1"
	FileDataPrefix = "file-v1-"
	FileLockPrefix = "lock-global-v1-"

	HKey = "hash_key"
	RKey = "range_key"
)

var SectorNotFoundErr = errors.New("sector not found")

type FileMetaV1V2 struct {
	MetaVersion int    `json:"meta_version"`
	SectorSize  int64  `json:"sector_size"`
	OrigName    string `json:"orig_name"`
	RandID      string `json:"rand_id"`
	DataRowKey  string `json:"data_row_key"`
	LockRowKey  string `json:"lock_row_key"`
	CompressAlg string `json:"compress_alg"`

	// v2 only fields
	FileSize int64    `json:"file_size"`
	Sectors  []string `json:"sectors"`
}
