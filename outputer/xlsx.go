package outputer

import (
	"sort"
	"strconv"

	"github.com/xuri/excelize/v2"
)

type xlsxOutputer[T Tablur] struct {
	path       string
	header     []string
	f          *excelize.File
	sheetName  string
	sheetIndex int
	cursor     int
}

func NewXLSX[T Tablur](path string) *xlsxOutputer[T] {
	return &xlsxOutputer[T]{
		path:       path,
		sheetName:  "Sheet1",
		sheetIndex: 0,
		cursor:     2,
	}
}

func (o *xlsxOutputer[T]) Init() error {
	f := excelize.NewFile()
	o.f = f
	return nil
}

func (o *xlsxOutputer[T]) Close() (err error) {
	o.f.SetActiveSheet(o.sheetIndex)
	if err := o.f.SaveAs(o.path); err != nil {
		return err
	}
	return o.f.Close()
}

func (o *xlsxOutputer[T]) initHeader(header []string) error {
	o.header = header
	sort.Strings(o.header)
	return o.f.SetSheetRow(o.sheetName, "A1", &o.header)
}

func (o *xlsxOutputer[T]) writeRow(value []any) error {
	cell := "A" + strconv.Itoa(o.cursor)
	err := o.f.SetSheetRow(o.sheetName, cell, &value)
	if err != nil {
		return err
	}
	o.cursor++
	return nil
}

func (o *xlsxOutputer[T]) Load(batch []T) (int, error) {
	if o.f == nil {
		if err := o.Init(); err != nil {
			return 0, err
		}
	}
	if len(batch) == 0 {
		return 0, nil
	}
	if o.header == nil {
		if err := o.initHeader(batch[0].GetHeader()); err != nil {
			return 0, err
		}
	}

	for _, record := range batch {
		row := record.GetValue()
		value := make([]any, 0)
		for _, col := range o.header {
			val, ok := row[col]
			if !ok {
				value = append(value, "")
				continue
			}
			valStr, err := toString(val)
			if err != nil {
				return 0, err
			}
			value = append(value, valStr)
		}

		if err := o.writeRow(value); err != nil {
			return 0, err
		}
	}
	return len(batch), nil
}
