package promutil

import (
	"fmt"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func BenchmarkLabelsCompressorV2Compress(b *testing.B) {
	var lc LabelsCompressorV2
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

func BenchmarkLabelsCompressorV2Decompress(b *testing.B) {
	var lc LabelsCompressorV2
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

func BenchmarkLabelsCompressorV210M(b *testing.B) {
	var lc LabelsCompressorV2

	for i := 0; i < 100_000; i++ {
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
