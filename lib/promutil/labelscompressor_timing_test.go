package promutil

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func BenchmarkLabelsCompressorCompressFastPath(b *testing.B) {
	lc := NewLabelsCompressor()
	series := newTestSeries(100, 10)

	var dst []byte
	for _, labels := range series {
		dst = dst[:0]
		dst = lc.Compress(dst, labels)
	}

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

func BenchmarkLabelsCompressorCompressSlowPath(b *testing.B) {
	series := newTestSeries(100, 10)

	b.ReportAllocs()
	b.SetBytes(int64(len(series)))

	for b.Loop() {
		var dst []byte
		lc := NewLabelsCompressor()
		dst = dst[:0]
		for _, labels := range series {
			dst = lc.Compress(dst, labels)
		}
		Sink.Add(uint64(len(dst)))
	}
}

func BenchmarkLabelsCompressorDecompress(b *testing.B) {
	lc := NewLabelsCompressor()
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

func BenchmarkPreload100kLabelsCompressorCompress(b *testing.B) {
	lc := NewLabelsCompressor()

	var dst []byte
	var labels []prompb.Label
	for i := 0; i < 100_000; i++ {
		dst = dst[:0]
		labels = labels[:0]

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
		lc.Decompress(labels, lc.Compress(dst, labels))
	}

	series := newTestSeries(10, 10)

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

func BenchmarkPreload100kLabelsCompressorDecompress(b *testing.B) {
	lc := NewLabelsCompressor()

	var dst []byte
	var labels []prompb.Label
	for i := 0; i < 100_000; i++ {
		dst = dst[:0]
		labels = labels[:0]

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
		lc.Decompress(labels, lc.Compress(dst, labels))
	}

	series := newTestSeries(10, 10)
	datas := make([][]byte, len(series))
	dst = dst[:0]
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
