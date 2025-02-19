package schedule

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/TCP404/esdumpcore/core"
	"github.com/TCP404/esdumpcore/outputer"
	"github.com/TCP404/eutil/etl"
	"golang.org/x/sync/errgroup"
)

type E = core.Hit
type L = core.Hit

type Scheduler struct {
	host      string
	username  string
	password  string
	index     string
	timeField string
	startTime time.Time
	endTime   time.Time
	output    string
	outputer  outputer.Outputer[L]
	condition *core.ESBodyBool
	chanSize  int
	client    *core.ESClient
}

func New(
	host, username, password, index, timeField string,
	startTime, endTime time.Time,
	output string, outputerHandler outputer.Outputer[L],
	condition *core.ESBodyBool,
) (*Scheduler, error) {
	chanSize := 1000
	client, err := core.NewClient([]string{host}, username, password, chanSize)
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		host:      host,
		username:  username,
		password:  password,
		index:     index,
		timeField: timeField,
		startTime: startTime,
		endTime:   endTime,
		output:    output,
		outputer:  outputerHandler,
		condition: condition,
		chanSize:  chanSize,
		client:    client,
	}, nil
}

func (s *Scheduler) String() string {
	return fmt.Sprintf(
		"host: %s \nusername: %s \npassword: %s \nindex: %s \ntimeField: %s \nstartTime: %s \nendTime: %s \noutput: %s \ncondition: %v \nchanSize: %d",
		s.host, s.username, s.password, s.index, s.timeField, s.startTime, s.endTime, s.output, s.condition, s.chanSize,
	)
}

func (s *Scheduler) BuildQuery() (*core.QueryConfig, error) {
	queryConfig, err := core.NewQueryConfig(
		core.WithIndex(s.index),
		core.WithTimeField(s.timeField),
		core.WithStartTime(s.startTime),
		core.WithEndTime(s.endTime),
		core.WithBody(s.handleCondition()),
	)
	if err != nil {
		return nil, err
	}
	return queryConfig, nil
}

func (s *Scheduler) Run(queryConfig *core.QueryConfig, outputHandler outputer.Outputer[L]) error {

	ctx, cancel := signal.NotifyContext(context.TODO(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	g, ctx := errgroup.WithContext(ctx)

	slog.Debug("query count body", slog.String("body", string(queryConfig.BodyBytes)))
	count, err := s.client.Count(ctx, queryConfig)
	if err != nil {
		cancel()
		return err
	}

	slog.Info("query count", slog.Int64("total:", count))
	if count == 0 {
		return nil
	}

	outputChan := make(chan core.M, s.chanSize)
	defer func() {
		if err := recover(); err != nil {
			// close channel if the outputChan is not closed
			_, ok := <-outputChan
			if ok {
				close(outputChan)
			}
		}
	}()
	s.outputer = outputHandler
	g.Go(func() (err error) {
		if err = s.outputer.Init(); err != nil {
			cancel()
			return err
		}
		defer func() {
			if err = s.outputer.Close(); err != nil {
				cancel()
			}
		}()
		if err = s.outputer.Output(ctx, outputChan); err != nil {
			cancel()
			return err
		}
		return nil
	})

	g.Go(func() error {
		defer func() { close(outputChan) }()

		return s.client.FindWithConsume(
			ctx,
			queryConfig.With(core.WithBatchSize(1000)),
			func(c chan core.Hit) {
				for v := range c {
					outputChan <- v.Source
				}
			},
		)
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) RunETL(ctx context.Context, queryConfig *core.QueryConfig, transformFunc etl.TransformFunc[E, L], total uint64) (err error) {
	if err := s.outputer.Init(); err != nil {
		return err
	}
	defer func() {
		s.outputer.Close()
	}()
	engine, err := s.BuildWithETL(queryConfig, transformFunc, total)
	if err != nil {
		return err
	}
	if engine == nil {
		return nil
	}
	if err := engine.Run(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Scheduler) BuildWithETL(
	queryConfig *core.QueryConfig, transformFunc etl.TransformFunc[E, L], total uint64,
	opts ...etl.Option[E, L],
) (ins *etl.ETL[E, L], err error) {
	etractFunc := func(ctx context.Context) (etl.Iterator[E], error) {
		return core.NewQueryIterator(ctx, s.client, queryConfig), nil
	}

	ins = etl.New(
		etractFunc,
		transformFunc,
		s.outputer.Load,
		append(
			[]etl.Option[E, L]{
				etl.WithReporter[E, L](etl.ProgressReporterFactory(total)),
				etl.WithSweepCSize[E, L](1000),
				etl.WithExtractBatchSize[E, L](100),
				etl.WithTransformBatchSize[E, L](100),
				etl.WithLoadBatchSize[E, L](100),
			},
			opts...,
		)...,
	)
	return ins, nil
}

func (s *Scheduler) Init() error {
	return s.outputer.Init()
}

func (s *Scheduler) Close() error {
	return s.outputer.Close()
}

func (s *Scheduler) QueryTotal(queryConfig *core.QueryConfig) (int64, error) {
	total, err := s.client.Count(context.TODO(), queryConfig)
	if err != nil {
		return 0, err
	}
	return total, nil
}

func (s *Scheduler) handleCondition() *core.ESBody {
	body := core.ESBody{
		Query: core.ESBodyQuery{
			Bool: core.ESBodyBool{},
		},
	}
	if s.condition != nil {
		body.Query.Bool.Filter = append(body.Query.Bool.Filter, s.condition.Filter...)
		body.Query.Bool.Must = append(body.Query.Bool.Must, s.condition.Must...)
		body.Query.Bool.MustNot = append(body.Query.Bool.MustNot, s.condition.MustNot...)
		body.Query.Bool.Should = append(body.Query.Bool.Should, s.condition.Should...)
	}
	return &body
}
