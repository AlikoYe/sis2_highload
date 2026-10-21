package domain

import "errors"

var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrNotFound        = errors.New("not found")
	ErrUnavailable     = errors.New("unavailable")
	ErrOverloaded      = errors.New("overloaded")
)
