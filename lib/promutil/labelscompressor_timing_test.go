package promutil

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func BenchmarkLabelsCompressorCompress(b *testing.B) {
	lc := NewLabelsCompressorV2()
	series := newTestSeries(100, 10)

	b.ReportAllocs()
	b.SetBytes(int64(len(series)))

	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		for pb.Next() {
			dst = dst[:0]
			for _, labels := range series {
				dst = lc.Compress(dst, labels)
			}
			Sink.Add(uint64(len(dst)))
		}
	})
}

func BenchmarkLabelsCompressorDecompress(b *testing.B) {
	lc := NewLabelsCompressorV2()
	series := newTestSeries(100, 10)
	datas := make([][]byte, len(series))
	var dst []byte
	for i, labels := range series {
		dstLen := len(dst)
		dst = lc.Compress(dst, labels)
		datas[i] = dst[dstLen:]
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(series)))

	b.RunParallel(func(pb *testing.PB) {
		var labels []prompb.Label
		for pb.Next() {
			for _, data := range datas {
				labels = lc.Decompress(labels[:0], data)
			}
			Sink.Add(uint64(len(labels)))
		}
	})
}

func BenchmarkPreload1MLabelsCompressorCompress(b *testing.B) {
	lc := NewLabelsCompressorV2()

	for i := 0; i < 1_000_000; i++ {
		labels := []prompb.Label{
			{
				Name:  "instance",
				Value: fmt.Sprintf("1.2.3.%d", i),
			},
			{
				Name:  "job",
				Value: fmt.Sprintf("pod%d", i),
			},
		}
		lc.Compress(nil, labels)
	}

	series := newTestSeries(100, 10)

	b.ReportAllocs()
	b.SetBytes(int64(len(series)))

	b.RunParallel(func(pb *testing.PB) {
		var dst []byte
		for pb.Next() {
			dst = dst[:0]
			for _, labels := range series {
				dst = lc.Compress(dst, labels)
			}
			Sink.Add(uint64(len(dst)))
		}
	})
}

func BenchmarkPreload1MLabelsCompressorDecompress(b *testing.B) {
	lc := NewLabelsCompressorV2()

	for i := 0; i < 1_000_000; i++ {
		labels := []prompb.Label{
			{
				Name:  "instance",
				Value: fmt.Sprintf("1.2.3.%d", i),
			},
			{
				Name:  "job",
				Value: fmt.Sprintf("pod%d", i),
			},
		}
		lc.Compress(nil, labels)
	}

	series := newTestSeries(100, 10)
	datas := make([][]byte, len(series))
	var dst []byte
	for i, labels := range series {
		dstLen := len(dst)
		dst = lc.Compress(dst, labels)
		datas[i] = dst[dstLen:]
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(series)))

	b.RunParallel(func(pb *testing.PB) {
		var labels []prompb.Label
		for pb.Next() {
			for _, data := range datas {
				labels = lc.Decompress(labels[:0], data)
			}
			Sink.Add(uint64(len(labels)))
		}
	})
}

var Sink atomic.Uint64
