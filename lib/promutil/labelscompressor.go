package promutil

import (
	"reflect"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/alphadose/haxmap"
	"github.com/cespare/xxhash/v2"
)

var hashBBP = bytesutil.ByteBufferPool{}

type currPrevMaps struct {
	idxToLabels     *haxmap.Map[uint64, prompb.Label] // map[uint64]prompb.Label
	prevIdxToLabels *haxmap.Map[uint64, prompb.Label] // map[uint64]prompb.Label
}

type LabelsCompressor struct {
	currPrevMaps atomic.Pointer[currPrevMaps]

	totalSizeBytes atomic.Uint64
	totalItems     atomic.Uint64
}

func NewLabelsCompressorV2() *LabelsCompressor {
	lc := &LabelsCompressor{}
	lc.currPrevMaps.Store(&currPrevMaps{
		idxToLabels:     haxmap.New[uint64, prompb.Label](1e6),
		prevIdxToLabels: haxmap.New[uint64, prompb.Label](1e6),
	})
	go lc.cleanupLoop()
	return lc
}

// SizeBytes returns the size of lc data in bytes
func (lc *LabelsCompressor) SizeBytes() uint64 {
	return uint64(unsafe.Sizeof(*lc)) + lc.totalSizeBytes.Load()
}

// ItemsCount returns the number of items in lc
func (lc *LabelsCompressor) ItemsCount() uint64 {
	return lc.totalItems.Load()
}

func (lc *LabelsCompressor) Compress(dst []byte, labels []prompb.Label) []byte {
	if len(labels) == 0 {
		// Fast path
		return append(dst, 0)
	}

	a := encoding.GetUint64s(len(labels) + 1)
	a.A[0] = uint64(len(labels))
	lc.compress(a.A[1:], labels)
	dst = encoding.MarshalVarUint64s(dst, a.A)
	encoding.PutUint64s(a)
	return dst
}

func (lc *LabelsCompressor) compress(dst []uint64, labels []prompb.Label) {
	if len(labels) == 0 {
		return
	}

	var maxSize int
	for i := range labels {
		maxSize = max(maxSize, len(labels[i].Name)+len(labels[i].Value))
	}
	maxSize += 1 // for '='

	bb := hashBBP.Get()
	defer hashBBP.Put(bb)
	bb.Grow(maxSize)

	idxToLabels, _ := lc.maps()

	_ = dst[len(labels)-1]

	var totalSizeBytes, totalItems uint64
	for i, label := range labels {
		bb.Reset()
		bb.Write(s2b(label.Name))
		bb.Write([]byte(`=`))
		bb.Write(s2b(label.Value))
		idx := xxhash.Sum64(bb.B)

		if _, ok := idxToLabels.Get(idx); !ok {
			labelCopy := cloneLabel(label)
			idxToLabels.Set(idx, labelCopy)

			// Update lc.totalSizeBytes
			labelSizeBytes := uint64(len(label.Name) + len(label.Value))
			entrySizeBytes := labelSizeBytes + uint64(2*(unsafe.Sizeof(label)+unsafe.Sizeof(&label))+unsafe.Sizeof(label))
			totalSizeBytes += entrySizeBytes

			totalItems += 1
		}

		dst[i] = idx
	}

	if totalItems > 0 {
		lc.totalSizeBytes.Add(totalSizeBytes)
		lc.totalItems.Add(totalItems)
	}
}

func cloneLabel(label prompb.Label) prompb.Label {
	// pre-allocate memory for label name and value
	n := len(label.Name) + len(label.Value)
	buf := make([]byte, 0, n)

	buf = append(buf, label.Name...)
	labelName := bytesutil.ToUnsafeString(buf)

	buf = append(buf, label.Value...)
	labelValue := bytesutil.ToUnsafeString(buf[len(labelName):])
	return prompb.Label{
		Name:  labelName,
		Value: labelValue,
	}
}

func (lc *LabelsCompressor) Decompress(dst []prompb.Label, src []byte) []prompb.Label {
	labelsLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal labels length from uvarint")
	}
	tail := src[nSize:]
	if labelsLen == 0 {
		// fast path - nothing to decode
		if len(tail) > 0 {
			logger.Panicf("BUG: unexpected non-empty tail left; len(tail)=%d; tail=%X", len(tail), tail)
		}
		return dst
	}

	a := encoding.GetUint64s(int(labelsLen))
	var err error
	tail, err = encoding.UnmarshalVarUint64s(a.A, tail)
	if err != nil {
		logger.Panicf("BUG: cannot unmarshal label indexes: %s", err)
	}
	if len(tail) > 0 {
		logger.Panicf("BUG: unexpected non-empty tail left: len(tail)=%d; tail=%X", len(tail), tail)
	}
	dst = lc.decompress(dst, a.A)
	encoding.PutUint64s(a)
	return dst
}

func (lc *LabelsCompressor) decompress(dst []prompb.Label, src []uint64) []prompb.Label {
	idxToLabels, prevIdxToLabels := lc.maps()

	for _, idx := range src {
		label0, ok := idxToLabels.Get(idx)
		if !ok {
			var ok bool
			label0, ok = prevIdxToLabels.Get(idx)
			if !ok {
				logger.Panicf("BUG: missing label for idx=%d", idx)
			}
			idxToLabels.Set(idx, label0)
		}
		dst = append(dst, label0)
	}
	return dst
}

func (lc *LabelsCompressor) cleanupLoop() {
	// ticker should be 3x bigger than any aggr interval
	t := time.NewTicker(time.Minute * 10)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			lc.cleanup()
		}
	}
}

func (lc *LabelsCompressor) cleanup() {
	idxToLabels, prevIdxToLabels := lc.maps()
	lc.currPrevMaps.Store(&currPrevMaps{
		idxToLabels:     haxmap.New[uint64, prompb.Label](1e6),
		prevIdxToLabels: idxToLabels,
	})

	var totalSizeBytes, totalItems uint64
	prevIdxToLabels.ForEach(func(idx uint64, label prompb.Label) bool {
		if _, loaded := idxToLabels.Get(idx); loaded {
			return true
		}

		// Update lc.totalSizeBytes
		labelSizeBytes := uint64(len(label.Name) + len(label.Value))
		entrySizeBytes := labelSizeBytes + uint64(2*(unsafe.Sizeof(label)+unsafe.Sizeof(&label))+unsafe.Sizeof(label))
		totalSizeBytes += entrySizeBytes

		totalItems += 1
		return true
	})

	if totalItems > 0 {
		lc.totalSizeBytes.Add(-totalSizeBytes)
		lc.totalItems.Add(-totalItems)
	}
}

func (lc *LabelsCompressor) maps() (*haxmap.Map[uint64, prompb.Label], *haxmap.Map[uint64, prompb.Label]) {
	maps := lc.currPrevMaps.Load()
	return maps.idxToLabels, maps.prevIdxToLabels
}

func s2b(s string) (b []byte) {
	strh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = strh.Data
	sh.Len = strh.Len
	sh.Cap = strh.Len
	return b
}
