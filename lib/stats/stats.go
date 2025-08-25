package stats

import (
	"fmt"
	"runtime"
	"syscall"
	"time"
)

// getCPUTime returns process user+system CPU time in seconds (works on macOS, BSD, Linux).
func getCPUTime() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	user := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return user + sys
}

func Measure() func() {
	var mStart runtime.MemStats
	runtime.ReadMemStats(&mStart)

	startCPU := getCPUTime()
	start := time.Now()

	return func() {
		elapsed := time.Since(start)

		var mEnd runtime.MemStats
		runtime.ReadMemStats(&mEnd)

		endCPU := getCPUTime()
		cpuTimeUsed := endCPU - startCPU
		cpuUtil := cpuTimeUsed / elapsed.Seconds() / float64(runtime.NumCPU()) * 100

		allocRate := float64(mEnd.TotalAlloc-mStart.TotalAlloc) / elapsed.Seconds()
		gcCount := mEnd.NumGC - mStart.NumGC
		gcCPU := mEnd.GCCPUFraction * 100

		fmt.Println("==== GC / CPU / Memory report ====")
		fmt.Printf("Elapsed: %v\n", elapsed)
		fmt.Printf("HeapAlloc: %d MB\n", mEnd.HeapAlloc/1024/1024)
		fmt.Printf("TotalAlloc: %d MB\n", mEnd.TotalAlloc/1024/1024)
		fmt.Printf("Alloc rate: %.2f MB/s\n", allocRate/1024/1024)
		fmt.Printf("GC cycles: %d\n", gcCount)
		fmt.Printf("GC CPU fraction: %.2f%%\n", gcCPU)
		if gcCount > 0 {
			fmt.Printf("Last GC pause: %v\n", time.Duration(mEnd.PauseNs[(mEnd.NumGC+255)%256]))
		}
		fmt.Printf("Process CPU utilization: %.2f%% of total cores\n", cpuUtil)
		fmt.Println("==================================")
	}
}
