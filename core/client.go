package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"

	"golang.org/x/sync/errgroup"
)

func doRequest(ctx context.Context, cli *elasticsearch.Client, req esapi.Request) (*ESResponse, error) {
	res, err := req.Do(ctx, cli)
	defer func() {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
	}()
	if err != nil {
		return nil, ESRequestErr(err)
	}
	if res.IsError() {
		errorInfo := make(map[string]interface{})
		if err = json.NewDecoder(res.Body).Decode(&errorInfo); err != nil {
			return nil, DecodeErr(err)
		}
		if b, err := json.Marshal(errorInfo); err == nil {
			return nil, ESResponseErr(nil, res.Status(), string(b))
		}
		return nil, ESResponseErr(nil, res.Status(), fmt.Sprintf("%+v", errorInfo))
	}
	var initResult ESResponse
	if err = json.NewDecoder(res.Body).Decode(&initResult); err != nil {
		return nil, DecodeErr(err)
	}
	return &initResult, nil
}

type ESClient struct {
	client   *elasticsearch.Client
	chanSize int
}

func NewClient(addresses []string, username, password string, chanSize int) (*ESClient, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
	})
	if err != nil {
		return nil, ESClientCreateErr(err)
	}
	if _, err := client.Ping(); err != nil {
		return nil, ESConnectErr(err)
	}
	ins := &ESClient{
		client:   client,
		chanSize: chanSize,
	}
	return ins, nil
}

func (e *ESClient) Count(ctx context.Context, query *QueryConfig) (int64, error) {
	req := esapi.CountRequest{
		Index: query.index,
		Body:  query.bodyReader,
	}
	resp, err := doRequest(ctx, e.client, req)
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

func (e *ESClient) ScrollWithConsume(ctx context.Context, query *QueryConfig, consumeFn ConsumeFunc) error {
	c := make(chan Hit, e.chanSize)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { consumeFn(c); return nil })
	g.Go(func() error { defer close(c); return e.scroll(ctx, c, query) })
	return g.Wait()
}

func (e *ESClient) scroll(ctx context.Context, c chan Hit, query *QueryConfig) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		req := esapi.SearchRequest{
			Index:  query.index,
			Body:   query.bodyReader,
			Scroll: query.scroll,
			Size:   &query.batchSize,
		}
		initResult, err := doRequest(ctx, e.client, req)
		if err != nil {
			return err
		}
		for _, v := range initResult.Hits.Hits {
			c <- v
		}
		return e.loopData(ctx, c, initResult.ScrollID)
	}
}

func (e *ESClient) loopData(ctx context.Context, c chan Hit, scrollId string) (err error) {
	if scrollId == "" {
		return nil
	}
	defer func() {
		err = e.clearScroll(ctx, scrollId)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			req := esapi.ScrollRequest{ScrollID: scrollId, Scroll: time.Minute * 3}
			result, err := doRequest(ctx, e.client, req)
			if err != nil {
				return err
			}

			if len(result.Hits.Hits) == 0 {
				return nil
			}
			for _, v := range result.Hits.Hits {
				c <- v
			}
			scrollId = result.ScrollID
			time.Sleep(time.Second) // don't be too fast
			// log.Printf("scrolling %v...", scrollId)
		}
	}
}

func (e *ESClient) clearScroll(ctx context.Context, scrollId string) error {
	if scrollId == "" {
		return nil
	}
	req := esapi.ClearScrollRequest{ScrollID: []string{scrollId}}
	_, err := doRequest(ctx, e.client, req)
	return err
}

func (e *ESClient) FindWithConsume(ctx context.Context, query *QueryConfig, consumeFn ConsumeFunc) error {
	c := make(chan Hit, e.chanSize)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { consumeFn(c); return nil })
	g.Go(func() error { defer close(c); return e.findWithConsume(ctx, c, query) })
	return g.Wait()
}

func (e *ESClient) findWithConsume(ctx context.Context, c chan Hit, query *QueryConfig) error {

	sTime := query.startTime
	eTime := query.startTime.Add(query.stepDuration)
	for sTime.Compare(eTime) < 0 {
		body, err := query.UpdateBodyTimeRange(sTime, eTime)
		if err != nil {
			return err
		}
		bodyReader, err := marshalBytesBreader(body)
		if err != nil {
			return err
		}
		slog.Info(fmt.Sprintf("querying %v ~ %v...", sTime, eTime))
		resp, err := doRequest(ctx, e.client, esapi.SearchRequest{
			Index: query.index,
			Body:  bodyReader,
			Size:  &query.body.Size,
		})
		if err != nil {
			return err
		}
		hits := resp.Hits
		for _, v := range hits.Hits {
			c <- v
		}

		if hits.Total.Value >= 10000 && hits.Total.Relation == "gte" {
			maxTime := hits.Hits[len(hits.Hits)-1].Source[query.timeField].(string)
			maxTimeT, err := time.Parse(ESDateFormat, maxTime)
			if err != nil {
				return err
			}
			sTime = maxTimeT
			// eTime don't need to change
			continue
		}
		sTime = eTime
		eTime = time.Unix(min(sTime.Add(query.stepDuration).Unix(), query.endTime.Unix()), 0)
	}
	return nil
}
