package core

import "github.com/TCP404/eutil/cerr"

type ESClientCreateError struct {
	*cerr.Err
}

func ESClientCreateErr(err error) ESClientCreateError {
	return ESClientCreateError{cerr.Wrap(err, "es client create error")}
}

type ESConnectError struct {
	*cerr.Err
}

func ESConnectErr(err error) ESConnectError {
	return ESConnectError{cerr.Wrap(err, "es connect error")}
}

type ESRequestError struct {
	*cerr.Err
}

func ESRequestErr(err error) ESRequestError {
	return ESRequestError{cerr.Wrap(err, "es request error")}
}

type ESResponseError struct {
	*cerr.Err
	status int
	body   string
}

func ESResponseErr(err error, status int, body string) ESResponseError {
	var nerr *cerr.Err
	if err != nil {
		nerr = cerr.Wrapf(err, "es response error. status: %v, body: %v", status, body)
	} else {
		nerr = cerr.Newf("es response error. status: %v, body: %v", status, body)
	}
	nerr.SetCode(status)
	return ESResponseError{
		status: status,
		body:   body,
		Err:    nerr,
	}
}

type DecodeError struct {
	*cerr.Err
}

func DecodeErr(err error) DecodeError {
	return DecodeError{cerr.Wrap(err, "decode error")}
}

type EncodeError struct {
	*cerr.Err
}

func EncodeErr(err error) EncodeError {
	return EncodeError{cerr.Wrap(err, "encode error")}
}

func UnmarshalErr(err error) *cerr.Err { return cerr.Wrap(err, "unmarshal error") }
func MarshalErr(err error) *cerr.Err   { return cerr.Wrap(err, "marshal error") }

func ESQueryVarifyErr(msg string) *cerr.Err {
	return cerr.Newf("es query body varify error. error: %v", msg)
}
