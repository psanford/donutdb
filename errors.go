package donutdb

import (
	"errors"
	"fmt"
	"strconv"
)

var UnimplementedErr = errors.New("method unimplemented")

var ToBeImplementedErr = errors.New("method needs to be implemented")

var dynamoDBerrPrefix = "com.amazonaws.dynamodb.v20120810#"

func resourceInUseErr(msg string) apiErr {
	return apiErr{
		typ:  "ResourceInUseException",
		msg:  msg,
		code: 400,
	}
}

func validationErr(msg string) apiErr {
	return apiErr{
		typ:  "ValidationException",
		msg:  msg,
		code: 400,
	}
}

type apiErr struct {
	typ  string
	msg  string
	code int
}

func (e apiErr) Error() string {
	return e.msg
}

func (e apiErr) MarshalJSON() ([]byte, error) {
	res := []byte(fmt.Sprintf(`{"__type":"%s%s","message":%s}`,
		dynamoDBerrPrefix, e.typ, strconv.Quote(e.msg)))

	return res, nil
}

func (e apiErr) HTTPCode() int {
	return 400
}

type HTTPError interface {
	Error() string
	MarshalJSON() ([]byte, error)
	HTTPCode() int
}
