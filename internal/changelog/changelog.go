package changelog

import "time"

type Record struct {
	Action   string `json:"action"`
	ArgName  string `json:"arg_name"`
	ArgFlags int    `json:"arg_flags"`

	FName string `json:"fname"`

	P   []byte `json:"p"`
	Off int64  `json:"off"`

	RetError error `json:"ret_error"`
	RetCount int   `json:"ret_count"`

	TS time.Time `json:"ts"`
}
