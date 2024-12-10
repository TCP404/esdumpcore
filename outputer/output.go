package outputer

import (
	"context"

	"github.com/TCP404/esdumpcore/core"
)

type Outputer[L any] interface {
	Output(ctx context.Context, pipeline chan core.M) error
	Load(batch []L) (int, error)
	Init() error
	Close() error
}

var _ Outputer[core.Hit] = (*csvOutputer[core.Hit])(nil)
