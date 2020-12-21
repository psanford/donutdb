package donutdb

import (
	"errors"
	"fmt"
	"strconv"
)

var UnimplementedErr = errors.New("method unimplemented")

var ToBeImplementedErr = errors.New("method needs to be implemented")

var dynamoDBerrPrefix = "com.amazonaws.dynamodb.v20120810#"

type resourceInUseErr struct {
	msg string
}

func (e resourceInUseErr) Error() string {
	return e.msg
}

func (e resourceInUseErr) MarshalJSON() ([]byte, error) {
	typ := "ResourceInUseException"

	res := []byte(fmt.Sprintf(`{"__type":"%s%s","message":%s}`,
		dynamoDBerrPrefix, typ, strconv.Quote(e.msg)))

	return res, nil
}

func (e resourceInUseErr) HTTPCode() int {
	return 400
}

type HTTPError interface {
	Error() string
	MarshalJSON() ([]byte, error)
	HTTPCode() int
}
