package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const ESDateFormat = "2006-01-02T15:04:05.999Z"

type ConsumeFunc func(chan Hit)

type M map[string]any

func (m M) String() string {
	if m == nil {
		return "{}"
	}
	var builder strings.Builder
	builder.WriteString("{")
	for k, v := range m {
		builder.WriteString(k)
		builder.WriteString(":")
		builder.WriteString(fmt.Sprintf("%+v", v))
		builder.WriteString(",")
	}
	builder.WriteString("}")
	return builder.String()
}

func (m M) GetHeader() []string {
	if len(m) == 0 {
		return nil
	}
	var header []string
	for k := range m {
		header = append(header, k)
	}
	return header
}

type Hit struct {
	ID     string  `json:"_id"`
	Type   string  `json:"_type"`
	Score  float64 `json:"_score"`
	Index  string  `json:"_index"`
	Source M       `json:"_source"`
}

func (h Hit) GetHeader() []string {
	var header []string
	for k := range h.Source {
		header = append(header, k)
	}
	return header
}

func (h Hit) GetValue() M {
	return h.Source
}

type BatchHit = []Hit

type Hits struct {
	Total struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	MaxScore float64  `json:"max_score"`
	Hits     BatchHit `json:"hits"`
}

type ESResponse struct {
	ScrollID string `json:"_scroll_id"`
	Took     int    `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Count    int64  `json:"count"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits         Hits `json:"hits"`
	Aggregations M    `json:"aggregations,omitempty"`
}

type ESBody struct {
	Query ESBodyQuery `json:"query"`
	Size  int         `json:"size,omitempty"`
	Aggs  M           `json:"aggs,omitempty"`
}

func (e *ESBody) SetSize(size int) *ESBody {
	e.Size = size
	return e
}
func (e *ESBody) SetAggs(aggs M) *ESBody {
	e.Aggs = aggs
	return e
}
func (e *ESBody) SetQuery(query ESBodyQuery) *ESBody {
	e.Query = query
	return e
}

func (e ESBody) String() string {
	b, _ := json.MarshalIndent(e, "", "  ")
	return string(b)
}

type ESBodyQuery struct {
	Bool ESBodyBool `json:"bool"`
}

func (e *ESBodyQuery) AddFilter(filter M) *ESBodyQuery {
	e.Bool.Filter = append(e.Bool.Filter, filter)
	return e
}
func (e *ESBodyQuery) AddMust(must M) *ESBodyQuery {
	e.Bool.Must = append(e.Bool.Must, must)
	return e
}

func (e *ESBodyQuery) AddMustNot(mustNot M) *ESBodyQuery {
	e.Bool.MustNot = append(e.Bool.MustNot, mustNot)
	return e
}

func (e *ESBodyQuery) AddShould(should M) *ESBodyQuery {
	e.Bool.Should = append(e.Bool.Should, should)
	return e
}

func (e *ESBodyQuery) SetBool(bool ESBodyBool) *ESBodyQuery {
	e.Bool = bool
	return e
}

type ESBodyBool struct {
	Filter  []M `json:"filter,omitempty"`
	Must    []M `json:"must,omitempty"`
	MustNot []M `json:"must_not,omitempty"`
	Should  []M `json:"should,omitempty"`
}

func (e *ESBodyBool) String() string {
	var builder strings.Builder
	builder.WriteString("{")
	builder.WriteString("Filter:[")
	for _, v := range e.Filter {
		builder.WriteString(v.String())
	}
	builder.WriteString("],")
	builder.WriteString("Must:[")
	for _, v := range e.Must {
		builder.WriteString(v.String())
	}
	builder.WriteString("],")
	builder.WriteString("MustNot:[")
	for _, v := range e.MustNot {
		builder.WriteString(v.String())
	}
	builder.WriteString("],")
	builder.WriteString("Should:[")
	for _, v := range e.Should {
		builder.WriteString(v.String())
	}
	builder.WriteString("]")
	return builder.String()
}

func marshalBytesBreader(src *ESBody) (*bytes.Reader, error) {
	b, err := json.Marshal(src)
	if err != nil {
		return nil, MarshalErr(err)
	}
	return bytes.NewReader(b), nil
}
