package promutil

import (
	"reflect"
	"sync"
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
	mux sync.Mutex
	//nextIdx atomic.Uint64

	currPrevMaps atomic.Pointer[currPrevMaps]

	//idxToLabels     atomic.Pointer[sync.Map] // map[uint64]prompb.Label
	//prevIdxToLabels atomic.Pointer[sync.Map] // map[uint64]prompb.Label
	//idToLabels sync.Map // map[uint64]*prompb.Label

	//prevIdToLabels map[uint64]*prompb.Label
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

	//rLocked := true
	//lc.mux.RLock()
	//defer func() {
	//	if rLocked {
	//		lc.mux.RUnlock()
	//	} else {
	//		lc.mux.Unlock()
	//	}
	//}()

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
	for i := range labels {
		bb.Reset()
		bb.Write(s2b(labels[i].Name))
		bb.Write([]byte(`=`))
		bb.Write(s2b(labels[i].Value))
		id := xxhash.Sum64(bb.B)

		if _, ok := idxToLabels.Get(id); !ok {
			//if rLocked {
			//	lc.mux.RUnlock()
			//	lc.mux.Lock()
			//	rLocked = false
			//}
			//if lc.labelsToId == nil {
			//	lc.labelsToId = make(map[prompb.Label]uint64)
			//}
			//if lc.idToLabels == nil {
			//	lc.idToLabels = make(map[uint64]*prompb.Label)
			//}
			//
			//idx := lc.nextIdx.Add(1)
			lc.mux.Lock()
			if _, ok = idxToLabels.Get(id); !ok {
				labelCopy := cloneLabel(labels[i])
				idxToLabels.Set(id, labelCopy)
			}
			lc.mux.Unlock()
		}

		dst[i] = id
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
			lc.mux.Lock()
			var ok bool
			label0, ok = prevIdxToLabels.Get(idx)
			if !ok {
				lc.mux.Unlock()
				logger.Panicf("BUG: missing label for idx=%d", idx)
			}
			idxToLabels.Set(idx, label0)
			lc.mux.Unlock()
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
	lc.mux.Lock()
	defer lc.mux.Unlock()

	idxToLabels, _ := lc.maps()
	lc.currPrevMaps.Store(&currPrevMaps{
		idxToLabels:     haxmap.New[uint64, prompb.Label](1e6),
		prevIdxToLabels: idxToLabels,
	})
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
