package core

import (
	"context"
	"testing"
	"time"
)

func Test_NewClient(t *testing.T) {
	_, err := NewClient(
		[]string{"http://localhost:9200"},
		"admin",
		"admin",
		20,
	)
	if err != nil {
		t.Errorf("NewClient() error = %v", err)
	}
}

func Test_ESClient_Scroll(t *testing.T) {
	cli, err := NewClient(
		[]string{"http://localhost:9200"},
		"admin",
		"admin",
		20,
	)
	if err != nil {
		t.Errorf("NewClient() error = %v", err)
	}
	start := time.Date(2024, time.November, 7, 0, 0, 0, 0, time.Local)
	end := time.Date(2024, time.November, 7, 0, 30, 0, 0, time.Local)
	conf, err := NewQueryConfig(
		WithIndex("clue_online_alias"),
		WithTimeField("insert_time"),
		WithStartTime(start),
		WithEndTime(end),
		WithBody(&ESBody{
			Query: ESBodyQuery{
				Bool: ESBodyBool{
					Filter: []M{
						{"term": M{"product.keyword": "小红书"}},
					},
				},
			},
		}),
	)
	if err != nil {
		t.Errorf("NewQueryConfig() error = %v", err)
	}
	cli.FindWithConsume(
		context.TODO(),
		conf,
		func(c chan Hit) {
			for hit := range c {
				t.Logf("hit: %v", hit.ID)
			}
		},
	)

}
