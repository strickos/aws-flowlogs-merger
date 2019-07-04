package util

import (
	"fmt"
	"log"
	"runtime"
	"strings"
)

/*
CurrentMemoryInfo returns information about the current state of memory usage
*/
func CurrentMemoryInfo() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

/*
PrintMemUsage prints information about memory usage to the system log
*/
func PrintMemUsage(prefix string) {
	builder := strings.Builder{}

	if len(prefix) > 0 {
		builder.WriteString(prefix)
	}

	mem := CurrentMemoryInfo()

	builder.WriteString(fmt.Sprintf("Alloc = %s", prettifyBytes(mem.Alloc)))
	builder.WriteString(fmt.Sprintf(", TotalAlloc = %s", prettifyBytes(mem.TotalAlloc)))
	builder.WriteString(fmt.Sprintf(", Sys = %s", prettifyBytes(mem.Sys)))
	builder.WriteString(fmt.Sprintf(", GCCount = %v", mem.NumGC))
	log.Println(builder.String())
}

func prettifyBytes(b uint64) string {
	const kb float64 = 1024
	const mb float64 = kb * kb
	const gb float64 = kb * mb
	const tb float64 = kb * gb
	const pb float64 = kb * tb
	const eb float64 = kb * pb

	bVal := float64(b)
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	} else if bVal < mb {
		return fmt.Sprintf("%.1f kB", bVal/kb)
	} else if bVal < gb {
		return fmt.Sprintf("%.1f MB", bVal/mb)
	} else if bVal < tb {
		return fmt.Sprintf("%.1f GB", bVal/gb)
	} else if bVal < pb {
		return fmt.Sprintf("%.1f TB", bVal/tb)
	} else if bVal < eb {
		return fmt.Sprintf("%.1f PB", bVal/pb)
	} else {
		return fmt.Sprintf("%.1f EB", bVal/eb)
	}
}
