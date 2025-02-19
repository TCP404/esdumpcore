package outputer

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strings"

	"github.com/TCP404/esdumpcore/core"
	"github.com/spf13/cast"
)

type Tablur interface {
	GetHeader() []string
	GetValue() core.M
}

type csvOutputer[T Tablur] struct {
	path   string
	header []string
	writer *csv.Writer
	f      *os.File
}

func NewCSV[T Tablur](path string) *csvOutputer[T] {
	return &csvOutputer[T]{path: path}
}

func (o *csvOutputer[T]) Init() error {
	var err error

	if o.f, err = os.Create(o.path); err != nil {
		return err
	}
	if stat, _ := os.Stat(o.path); stat.IsDir() {
		return errors.New("output path is a directory not a file")
	}

	o.writer = csv.NewWriter(bufio.NewWriter(o.f))
	return nil
}

func (o *csvOutputer[T]) Close() (err error) {
	if o.writer != nil {
		err = o.writer.Error()
		o.writer.Flush()
	}
	if o.f != nil {
		err = errors.Join(o.f.Close())
	}
	return err
}

func (o *csvOutputer[T]) initHeader(header []string) {
	o.header = header
	sort.Strings(o.header)
	o.writer.Write(o.header)
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		se, err := cast.ToStringE(val)
		if err != nil {
			b, _ := json.Marshal(val)
			se = string(b)
		}
		return se
	}
}

func (o *csvOutputer[T]) Load(batch []T) (int, error) {
	if o.writer == nil || o.f == nil {
		if err := o.Init(); err != nil {
			return 0, err
		}
	}

	if len(batch) == 0 {
		return 0, nil
	}
	if len(o.header) == 0 {
		o.initHeader(batch[0].GetHeader())
	}

	for _, v := range batch {
		row := v.GetValue()
		value := make([]string, 0)
		for _, k := range o.header {
			val, ok := row[k]
			if !ok {
				value = append(value, "")
				continue
			}
			value = append(value, FormatCSV(toString(val)))
		}
		o.writer.Write(value)
	}
	return len(batch), nil
}

func (o *csvOutputer[T]) Output(ctx context.Context, pipeline chan core.M) (err error) {
	for {
		select {
		case <-ctx.Done():
			return nil
		case row, ok := <-pipeline:
			if !ok {
				return nil
			}
			if len(o.header) == 0 {
				o.initHeader(row.GetHeader())
			}
			value := make([]string, 0)
			for _, k := range o.header {
				val, ok := row[k]
				if !ok {
					value = append(value, "")
					continue
				}
				value = append(value, FormatCSV(cast.ToString(val)))
			}
			o.writer.Write(value)
		}
	}
}

func FormatCSV(val string) string {
	for _, repl := range [][2]string{
		{",", "，"},
		{"\n", " "},
		{"\r", " "},
		{"\t", " "},
	} {
		src, dst := repl[0], repl[1]
		val = strings.ReplaceAll(val, src, dst)
	}
	return val
}
