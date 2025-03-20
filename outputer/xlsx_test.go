package outputer

import (
	"testing"

	"github.com/TCP404/esdumpcore/core"
)

func Test_xlsxOutputer_Load(t *testing.T) {
	var (
		name  string = "xlsxOutputer.Load"
		batch        = []core.Hit{
			{Source: map[string]any{"name": "test1", "age": 31}},
			{Source: map[string]any{"name": "test2", "age": 32}},
			{Source: map[string]any{"name": "test3", "age": 33}},
		}
		want    int  = len(batch)
		wantErr bool = false
	)

	t.Run(name, func(t *testing.T) {
		o := NewXLSX[core.Hit]("./test.xlsx")
		got, gotErr := o.Load(batch)
		if gotErr != nil {
			if !wantErr {
				t.Errorf("Load() failed: %v", gotErr)
			}
			return
		}
		if wantErr {
			t.Fatal("Load() succeeded unexpectedly")
		}
		if got != want {
			t.Errorf("Load() = %v, want %v", got, want)
		}
		o.Close()
	})

}
