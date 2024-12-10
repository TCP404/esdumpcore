package core

import (
	"context"
	"log/slog"
	"time"

	"github.com/TCP404/eutil/etl"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/pkg/errors"
)

type T = Hit
type QueryIterator struct {
	sTime        time.Time
	eTime        time.Time
	err          error
	query        *QueryConfig
	client       *elasticsearch.Client
	ctx          context.Context
	tmpContainer BatchHit
	idx          int
}

var _ etl.Iterator[T] = (*QueryIterator)(nil)

func NewQueryIterator(ctx context.Context, client *ESClient, query *QueryConfig) etl.Iterator[T] {
	return &QueryIterator{
		sTime:  query.startTime,
		eTime:  query.endTime,
		query:  query,
		client: client.client,
		ctx:    ctx,
	}
}

func (i *QueryIterator) Err() error {
	return i.err
}

func (i *QueryIterator) Next() bool {
	if i.err != nil { // stop when error
		return false
	}

	if i.tmpContainer == nil { // first time
		i.tmpContainer = i.queryValue()
		i.idx = 0
		return i.tmpContainer != nil
	}

	if i.idx < len(i.tmpContainer) { // still has value
		return true
	}

	if i.sTime.Before(i.eTime) { // need to query next batch
		i.tmpContainer = i.queryValue()
		i.idx = 0
		return i.tmpContainer != nil // if nil, means no more value
	}
	// no more value, stop
	i.tmpContainer = nil
	i.idx = -1
	return false
}

func (i *QueryIterator) Value() T {
	if i.idx == 999 {
		slog.Debug("query batch hits", slog.Int("hits", len(i.tmpContainer)))
	}
	hit := i.tmpContainer[i.idx]
	i.idx++
	return hit
}

func (i *QueryIterator) queryValue() BatchHit {
	body, err := i.query.UpdateBodyTimeRange(i.sTime, i.eTime)
	if err != nil {
		i.err = errors.Wrap(err, "update body time range error")
		return nil
	}

	bodyReader, err := marshalBytesBreader(body)
	if err != nil {
		i.err = errors.Wrap(err, "marshal body error")
		return nil
	}

	slog.Debug("query time range", slog.String("start", i.sTime.Format(ESDateFormat)), slog.String("end", i.eTime.Format(ESDateFormat)))
	resp, err := doRequest(i.ctx, i.client, esapi.SearchRequest{
		Index: i.query.index,
		Body:  bodyReader,
		Size:  &i.query.body.Size,
		Sort: []string{i.query.timeField+":asc"},
	})
	if err != nil {
		i.err = errors.Wrap(err, "do request error")
		return nil
	}

	hits := resp.Hits
	if hits.Total.Value >= 10000 && hits.Total.Relation == "gte" {
		maxTime := hits.Hits[len(hits.Hits)-1].Source[i.query.timeField].(string)
		maxTimeT, err := time.Parse(time.RFC3339, maxTime)
		if err != nil {
			i.err = errors.Wrap(err, "parse max time error")
			i.tmpContainer = hits.Hits
			return hits.Hits
		}

		i.sTime = maxTimeT
		// eTime don't need to change
	} else {
		// step forward
		i.sTime = i.eTime
		i.eTime = time.Unix(min(i.sTime.Add(i.query.stepDuration).Unix(), i.query.endTime.Unix()), 0)
	}

	if len(hits.Hits) == 0 {
		return nil
	}
	return hits.Hits
}
