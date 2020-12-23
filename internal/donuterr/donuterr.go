package donuterr

import (
	"errors"
	"fmt"
	"strconv"
)

var UnimplementedErr = errors.New("method unimplemented")

var ToBeImplementedErr = errors.New("method needs to be implemented")

var dynamoDBerrPrefix = "com.amazonaws.dynamodb.v20120810#"

func ResourceInUseErr(msg string) APIErr {
	return APIErr{
		typ:  "ResourceInUseException",
		msg:  msg,
		code: 400,
	}
}

func ValidationErr(msg string) APIErr {
	return APIErr{
		typ:  "ValidationException",
		msg:  msg,
		code: 400,
	}
}

func ResourceNotFoundErr(msg string) APIErr {
	return APIErr{
		typ:  "ResourceNotFoundException",
		msg:  msg,
		code: 400,
	}
}

func FieldNotImplementedErr(field string) error {
	return fmt.Errorf("%s not yet implemented in DonutDB", field)
}

type APIErr struct {
	typ  string
	msg  string
	code int
}

func (e APIErr) Error() string {
	return e.msg
}

func (e APIErr) MarshalJSON() ([]byte, error) {
	res := []byte(fmt.Sprintf(`{"__type":"%s%s","message":%s}`,
		dynamoDBerrPrefix, e.typ, strconv.Quote(e.msg)))

	return res, nil
}

func (e APIErr) HTTPCode() int {
	return 400
}

type HTTPError interface {
	Error() string
	MarshalJSON() ([]byte, error)
	HTTPCode() int
}
