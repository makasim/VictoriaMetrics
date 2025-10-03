package promremotewrite

import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert/relabel"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/auth"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/promremotewrite/stream"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/protoparserutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/storage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/tenantmetrics"
	"github.com/VictoriaMetrics/metrics"
	"github.com/makasim/backpressure"
)

//var bp = newBP()

var okTotal = metrics.NewCounter(`vm_rows_inserted_ok_total`)
var deniedTotal = metrics.NewCounter(`vm_rows_inserted_denied_total`)
var congestedTotal = metrics.NewCounter(`vm_rows_inserted_congested_total`)

func newBP() *backpressure.Backpreassure {
	bp, err := backpressure.New(backpressure.Config{
		MaxMax:          1000,
		Max:             50,
		MinMax:          2,
		DecidePeriod:    time.Second * 3,
		IncreasePercent: 0.04,
		DecreasePercent: 0.12,

		//DecreaseLatency:           time.Second * 5,
		//DecreaseLatencyPercentile: 0.99,
		//SameLatency:               time.Second * 4,
		//SameLatencyPercentile:     0.99,
	})
	if err != nil {
		panic(fmt.Sprintf("BUG: Failed to create new backpressure AIMD: %s", err))
	}

	metrics.NewGauge(`vm_rows_inserted_backpressure_capacity_limit`, func() float64 {
		stats := bp.Stats()
		return float64(stats.Max)
	})

	metrics.NewGauge(`vm_rows_inserted_backpressure_capacity_used`, func() float64 {
		stats := bp.Stats()
		return float64(stats.Used)
	})

	bpSuccessfulTotal := metrics.NewCounter(`vm_rows_inserted_backpressure_successful_total`)
	bpDeniedTotal := metrics.NewCounter(`vm_rows_inserted_backpressure_denied_total`)
	bpDecideDecreaseTotal := metrics.NewCounter(`vm_rows_inserted_backpressure_decide_decrease_total`)
	bpDecideIncreaseTotal := metrics.NewCounter(`vm_rows_inserted_backpressure_decide_increase_total`)

	go func() {
		t := time.NewTicker(time.Second)
		for range t.C {
			stats := bp.Stats()
			bpSuccessfulTotal.Set(uint64(stats.SuccessfulCounter))
			bpDeniedTotal.Set(uint64(stats.DeniedCounter))
			bpDecideDecreaseTotal.Set(uint64(stats.DecideDecreaseCounter))
			bpDecideIncreaseTotal.Set(uint64(stats.DecideIncreaseCounter))
		}

	}()

	return bp
}

var hMux sync.Mutex
var h = newHistogram()

func newHistogram() *hdrhistogram.WindowedHistogram {
	h := hdrhistogram.NewWindowed(2, 0, 120000, 3)

	go func() {
		t := time.NewTicker(10 * time.Second)
		for range t.C {
			hMux.Lock()
			h.Rotate()

			mh := h.Merge()

			valToDur := func(v int64) time.Duration {
				return time.Duration(v) * time.Millisecond
			}

			fmt.Fprintf(os.Stdout, "min=%v\tp50=%v\tp80=%v\tp95=%v\tp99=%v\tmax=%v\n",
				valToDur(mh.Min()), valToDur(mh.ValueAtQuantile(50)), valToDur(mh.ValueAtQuantile(80)),
				valToDur(mh.ValueAtQuantile(95)), valToDur(mh.ValueAtQuantile(99)), valToDur(mh.Max()))
			hMux.Unlock()
		}
	}()

	return h
}

var (
	rowsInserted       = metrics.NewCounter(`vm_rows_inserted_total{type="promremotewrite"}`)
	rowsTenantInserted = tenantmetrics.NewCounterMap(`vm_tenant_inserted_rows_total{type="promremotewrite"}`)
	rowsPerInsert      = metrics.NewHistogram(`vm_rows_per_insert{type="promremotewrite"}`)
)

// InsertHandler processes remote write for prometheus.
func InsertHandler(at *auth.Token, req *http.Request) error {
	//var bpt backpressure.Token
	if *netstorage.BackpressureEnabled {
		//var allow bool
		//bpt, allow = bp.Acquire()
		//if !allow {
		//	deniedTotal.Inc()
		//	return &httpserver.ErrorWithStatusCode{
		//		Err:        fmt.Errorf("cannot insert rows now - too many concurrent requests"),
		//		StatusCode: http.StatusServiceUnavailable,
		//	}
		//}

		if netstorage.ApproachingMaxCapacity() {
			deniedTotal.Inc()
			return &httpserver.ErrorWithStatusCode{
				Err:        fmt.Errorf("cannot insert rows now - too many concurrent requests"),
				StatusCode: http.StatusServiceUnavailable,
			}
		}

		//timeoutT := time.NewTimer(time.Second * 55)
		//defer timeoutT.Stop()
		//
		//tryT := time.NewTicker(time.Millisecond * 500)
		//defer tryT.Stop()
		//for {
		//	var allow bool
		//	bpt, allow = bp.Acquire()
		//	if allow {
		//		break
		//	}
		//
		//	select {
		//	case <-tryT.C:
		//		continue
		//	case <-timeoutT.C:
		//		deniedTotal.Inc()
		//		return &httpserver.ErrorWithStatusCode{
		//			Err:        fmt.Errorf("cannot insert rows now - too many concurrent requests"),
		//			StatusCode: http.StatusServiceUnavailable,
		//		}
		//	}
		//}
	}
	//

	var err error

	start := time.Now()
	defer func() {
		if *netstorage.BackpressureEnabled {
			if err != nil {
				//bpt.Congested = true
				congestedTotal.Inc()
			}
			//bp.Release(bpt)
		}

		hMux.Lock()
		if err := h.Current.RecordValue(time.Since(start).Milliseconds()); err != nil {
			logger.Fatalf("BUG: cannot record insert start time in hdrhistogram: %s", err)
		}
		hMux.Unlock()
	}()

	extraLabels, err1 := protoparserutil.GetExtraLabels(req)
	if err1 != nil {
		return err1
	}
	isVMRemoteWrite := req.Header.Get("Content-Encoding") == "zstd"
	err = stream.Parse(req.Body, isVMRemoteWrite, func(tss []prompb.TimeSeries, _ []prompb.MetricMetadata) error {
		return insertRows(at, tss, extraLabels)
	})

	if err == nil {
		okTotal.Inc()
	}

	return err
}

func insertRows(at *auth.Token, timeseries []prompb.TimeSeries, extraLabels []prompb.Label) error {
	ctx := netstorage.GetInsertCtx()
	defer netstorage.PutInsertCtx(ctx)

	ctx.Reset() // This line is required for initializing ctx internals.
	rowsTotal := 0
	perTenantRows := make(map[auth.Token]int)
	hasRelabeling := relabel.HasRelabeling()
	for i := range timeseries {
		ts := &timeseries[i]
		rowsTotal += len(ts.Samples)
		ctx.Labels = ctx.Labels[:0]
		srcLabels := ts.Labels
		for _, srcLabel := range srcLabels {
			ctx.AddLabel(srcLabel.Name, srcLabel.Value)
		}
		for j := range extraLabels {
			label := &extraLabels[j]
			ctx.AddLabel(label.Name, label.Value)
		}

		if !ctx.TryPrepareLabels(hasRelabeling) {
			continue
		}
		atLocal := ctx.GetLocalAuthToken(at)
		storageNodeIdx := ctx.GetStorageNodeIdx(atLocal, ctx.Labels)
		ctx.MetricNameBuf = ctx.MetricNameBuf[:0]
		samples := ts.Samples
		for i := range samples {
			r := &samples[i]
			if len(ctx.MetricNameBuf) == 0 {
				ctx.MetricNameBuf = storage.MarshalMetricNameRaw(ctx.MetricNameBuf[:0], atLocal.AccountID, atLocal.ProjectID, ctx.Labels)
			}
			if err := ctx.WriteDataPointExt(storageNodeIdx, ctx.MetricNameBuf, r.Timestamp, r.Value); err != nil {
				return err
			}
		}
		perTenantRows[*atLocal] += len(ts.Samples)
	}
	rowsInserted.Add(rowsTotal)
	rowsTenantInserted.MultiAdd(perTenantRows)
	rowsPerInsert.Update(float64(rowsTotal))
	return ctx.FlushBufs()
}
