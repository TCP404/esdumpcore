package core

import (
	"bytes"
	"io"
	"time"

	"encoding/json"

	"github.com/TCP404/eutil"
)

type QueryConfig struct {
	QueryParam
	ScrollConfig
	SearchConfig
}

type OptFn func(*QueryConfig)

func NewQueryConfig(opt ...OptFn) (*QueryConfig, error) {
	q := new(QueryConfig)
	q.batchSize = 1000
	for _, o := range opt {
		o(q)
	}
	if q.timeField == "" {
		return nil, ESQueryVarifyErr("time field is required")
	}
	if q.startTime.IsZero() {
		return nil, ESQueryVarifyErr("start time is required")
	}
	if q.endTime.IsZero() {
		return nil, ESQueryVarifyErr("end time is required")
	}
	if q.stepByDay == 0 {
		q.stepDuration = q.endTime.Sub(q.startTime)
	}
	q.body.Query.Bool.Filter = append(q.body.Query.Bool.Filter, M{
		"range": M{
			q.timeField: M{
				"gte": q.startTime.Format(ESDateFormat),
				"lt":  q.endTime.Format(ESDateFormat),
			},
		},
	})

	b, err := json.Marshal(q.body)
	if err != nil {
		return nil, MarshalErr(err)
	}
	q.BodyBytes = b
	q.bodyReader = bytes.NewReader(b)
	return q, nil
}

func (q *QueryConfig) With(opt ...OptFn) *QueryConfig {
	for _, o := range opt {
		o(q)
	}
	return q
}

func (q *QueryConfig) UpdateBodyTimeRange(startTime, endTime time.Time) (*ESBody, error) {
	var newBody = new(ESBody)
	if err := eutil.DeepCopy(*q.body, newBody); err != nil {
		return nil, err
	}
	filters := make([]M, 0, len(newBody.Query.Bool.Filter))
	for _, v := range newBody.Query.Bool.Filter {
		if _, ok := v["range"]; ok {
			filters = append(filters, M{
				"range": M{
					q.timeField: M{
						"gte": startTime.Format(ESDateFormat),
						"lt":  endTime.Format(ESDateFormat),
					},
				},
			})
			continue
		}
		filters = append(filters, v)
	}
	newBody.Query.Bool.Filter = filters
	return newBody, nil
}

type ScrollConfig struct {
	scroll    time.Duration
	batchSize int
}

func WithScroll(scroll time.Duration) OptFn {
	return func(c *QueryConfig) {
		c.scroll = scroll
	}
}

func WithBatchSize(batchSize int) OptFn {
	return func(c *QueryConfig) {
		c.batchSize = batchSize
	}
}

type SearchConfig struct {
	stepByDay    int // 0 to not step
	stepDuration time.Duration
}

func WithStepByDay(stepByDay int) OptFn {
	return func(c *QueryConfig) {
		c.stepByDay = stepByDay
		c.stepDuration = time.Duration(stepByDay * 24 * int(time.Hour))
	}
}

type QueryParam struct {
	index      []string
	body       *ESBody
	BodyBytes  []byte
	timeField  string
	startTime  time.Time
	endTime    time.Time
	bodyReader io.Reader
}

func WithIndex(index string) OptFn {
	return func(c *QueryConfig) {
		c.index = []string{index}
	}
}

func WithTimeField(timeField string) OptFn {
	return func(c *QueryConfig) {
		c.timeField = timeField
	}
}

func WithStartTime(startTime time.Time) OptFn {
	return func(c *QueryConfig) {
		c.startTime = startTime
	}
}

func WithEndTime(endTime time.Time) OptFn {
	return func(c *QueryConfig) {
		c.endTime = endTime
	}
}

func WithBody(body *ESBody) OptFn {
	return func(c *QueryConfig) {
		c.body = body
	}
}

func WithSize(size int) OptFn {
	return func(c *QueryConfig) {
		c.body.Size = size
	}
}