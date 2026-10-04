package main

import (
	"context"
	"time"
)

type Store interface {
	Ping(context.Context) error
	Reset(context.Context) error
	Write(context.Context, []Event) (time.Duration, error)
	Read(context.Context, ReadQuery) ([]Event, time.Duration, error)
	Explain(context.Context, ReadQuery) (any, error)
	Stats(context.Context) (any, error)
	Maintain(context.Context) (any, error)
	Close()
}
