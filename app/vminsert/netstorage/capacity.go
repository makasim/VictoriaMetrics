package netstorage

//
//// DEBUG
//
//var hMux sync.Mutex
//var h = newHistogram()
//
//func newHistogram() *hdrhistogram.WindowedHistogram {
//	h := hdrhistogram.NewWindowed(2, 0, 1200000000, 3)
//
//	go func() {
//		t := time.NewTicker(10 * time.Second)
//		for range t.C {
//			hMux.Lock()
//			h.Rotate()
//
//			mh := h.Merge()
//
//			valToDur := func(v int64) float64 {
//				return float64(v) / 1e6
//			}
//
//			fmt.Fprintf(os.Stdout, "SENT-TO-STORE min=%.2f\tp50=%.2f\tp80=%.2f\tp95=%.2f\tp99=%.2f\tmax=%.2f\n",
//				valToDur(mh.Min()), valToDur(mh.ValueAtQuantile(50)), valToDur(mh.ValueAtQuantile(80)),
//				valToDur(mh.ValueAtQuantile(95)), valToDur(mh.ValueAtQuantile(99)), valToDur(mh.Max()))
//			hMux.Unlock()
//		}
//	}()
//
//	return h
//}
