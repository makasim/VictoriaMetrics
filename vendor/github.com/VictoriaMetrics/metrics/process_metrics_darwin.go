//go:build darwin

package metrics

import "C"
import (
	"io"
	"log"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func writeProcessMetrics(w io.Writer) {
	if procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", os.Getpid()); err == nil {
		if len(procs) == 1 {
			startTime := float64(procs[0].Proc.P_starttime.Nano() / 1e9)
			WriteGaugeUint64(w, "process_start_time_seconds", uint64(startTime))
		} else {
			log.Printf("ERROR: metrics: sysctl() returned %d proc structs (expected 1)", len(procs))
		}
	} else {
		log.Printf("ERROR: metrics: %s", err)
	}

	// The proc structure returned by kern.proc.pid above has an Rusage member,
	// but it is not filled in, so it needs to be fetched by getrusage(2).  For
	// that call, the UTime, STime, and Maxrss members are filled out, but not
	// Ixrss, Idrss, or Isrss for the memory usage.  Memory stats will require
	// access to the C API to call task_info(TASK_BASIC_INFO).
	rusage := unix.Rusage{}

	if err := unix.Getrusage(syscall.RUSAGE_SELF, &rusage); err == nil {
		cpuTime := time.Duration(rusage.Stime.Nano() + rusage.Utime.Nano()).Seconds()
		WriteGaugeFloat64(w, "process_cpu_seconds_total", cpuTime)
	} else {
		log.Printf("ERROR: metrics: %s", err)
	}

	//if rss, vsize, err := getMemory(); err == nil {
	//	WriteGaugeFloat64(w, "process_resident_memory_bytes", rss)
	//	WriteGaugeFloat64(w, "process_virtual_memory_bytes", vsize)
	//} else {
	//	log.Printf("ERROR: metrics: %s", err)
	//}

	if addressSpace, err := getSoftLimit(syscall.RLIMIT_AS); err == nil {
		WriteGaugeFloat64(w, "process_virtual_memory_max_bytes", float64(addressSpace))
	} else {
		log.Printf("ERROR: metrics: %s", err)
	}
}

func writeFDMetrics(w io.Writer) {
	if fds, err := getOpenFileCount(); err == nil {
		WriteGaugeFloat64(w, "process_open_fds", fds)
	} else {
		log.Printf("ERROR: metrics: %s", err)
	}

	if openFiles, err := getSoftLimit(syscall.RLIMIT_NOFILE); err == nil {
		WriteGaugeFloat64(w, "process_max_fds", float64(openFiles))
	} else {
		log.Printf("ERROR: metrics: %s", err)
	}
}

//func getMemory() (float64, float64, error) {
//	var rss, vsize C.ulonglong
//
//	if err := C.get_memory_info(&rss, &vsize); err != 0 {
//		return 0, 0, fmt.Errorf("task_info() failed with 0x%x", int(err))
//	}
//
//	return float64(vsize), float64(rss), nil
//}

func getOpenFileCount() (float64, error) {
	// Alternately, the undocumented proc_pidinfo(PROC_PIDLISTFDS) can be used to
	// return a list of open fds, but that requires a way to call C APIs.  The
	// benefits, however, include fewer system calls and not failing when at the
	// open file soft limit.

	if dir, err := os.Open("/dev/fd"); err != nil {
		return 0.0, err
	} else {
		defer dir.Close()

		// Avoid ReadDir(), as it calls stat(2) on each descriptor.  Not only is
		// that info not used, but KQUEUE descriptors fail stat(2), which causes
		// the whole method to fail.
		if names, err := dir.Readdirnames(0); err != nil {
			return 0.0, err
		} else {
			// Subtract 1 to ignore the open /dev/fd descriptor above.
			return float64(len(names) - 1), nil
		}
	}
}

func getSoftLimit(which int) (uint64, error) {
	rlimit := syscall.Rlimit{}

	if err := syscall.Getrlimit(which, &rlimit); err != nil {
		return 0, err
	}

	return rlimit.Cur, nil
}
