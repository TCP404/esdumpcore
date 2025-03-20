package outputer

import (
	"github.com/TCP404/esdumpcore/core"
)

type Outputer[L any] interface {
	Load(batch []L) (int, error)
	Init() error
	Close() error
}

var _ Outputer[core.Hit] = (*csvOutputer[core.Hit])(nil)
var _ Outputer[core.Hit] = (*xlsxOutputer[core.Hit])(nil)
