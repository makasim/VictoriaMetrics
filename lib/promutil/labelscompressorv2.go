package promutil

import (
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

type LabelsCompressorV2 struct {
	mux sync.RWMutex

	nextIdx atomic.Uint64

	labelsToId sync.Map // map[prompb.Label]uint64
	idToLabels map[uint64]*prompb.Label

	prevIdToLabels map[uint64]*prompb.Label
}

func (lc *LabelsCompressorV2) Compress(dst []byte, labels []prompb.Label) []byte {
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

func (lc *LabelsCompressorV2) compress(dst []uint64, labels []prompb.Label) {
	if len(labels) == 0 {
		return
	}

	rLocked := true
	lc.mux.RLock()
	defer func() {
		if rLocked {
			lc.mux.RUnlock()
		} else {
			lc.mux.Unlock()
		}
	}()

	_ = dst[len(labels)-1]
	for i := range labels {
		v, ok := lc.labelsToId.Load(labels[i])
		if !ok {
			if rLocked {
				lc.mux.RUnlock()
				lc.mux.Lock()
				rLocked = false
			}
			//if lc.labelsToId == nil {
			//	lc.labelsToId = make(map[prompb.Label]uint64)
			//}
			if lc.idToLabels == nil {
				lc.idToLabels = make(map[uint64]*prompb.Label)
			}

			idx := lc.nextIdx.Add(1)
			labelCopy := cloneLabel(labels[i])
			lc.labelsToId.Store(labelCopy, idx)
			lc.idToLabels[idx] = &labelCopy

			v = idx
		}

		dst[i] = v.(uint64)
	}
}

func (lc *LabelsCompressorV2) Decompress(dst []prompb.Label, src []byte) []prompb.Label {
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

func (lc *LabelsCompressorV2) decompress(dst []prompb.Label, src []uint64) []prompb.Label {
	lc.mux.RLock()
	defer lc.mux.RUnlock()

	for _, idx := range src {
		label, ok := lc.idToLabels[idx]
		if !ok {
			logger.Panicf("BUG: missing label for idx=%d", idx)
		}
		dst = append(dst, *label)
	}
	return dst
}

//func (lc *LabelsCompressorV2) cleanupLoop() {
//	// ticker should be 3x bigger than any aggr interval
//	t := time.NewTicker(time.Hour)
//	defer t.Stop()
//	for {
//		select {
//		case <-t.C:
//			lc.cleanup()
//		}
//	}
//}
//
//func (lc *LabelsCompressorV2) cleanup() {
//	lc.mux.Lock()
//	defer lc.mux.Unlock()
//
//	prevIdToLabels := lc.prevIdToLabels
//	lc.prevIdToLabels = lc.idToLabels
//	lc.idToLabels = make(map[uint64]*prompb.Label)
//
//	for _, label := range prevIdToLabels {
//		delete(lc.labelsToId, *label)
//	}
//}
