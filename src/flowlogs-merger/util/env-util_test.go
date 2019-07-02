package util

import (
	"testing"
)

func Test_PrettifyBytes_SubK(t *testing.T) {
	str := prettifyBytes(900)
	expected := "900 B"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_KB(t *testing.T) {
	str := prettifyBytes(1286)
	expected := "1.3 kB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_MB(t *testing.T) {
	str := prettifyBytes(1286654)
	expected := "1.2 MB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_GB(t *testing.T) {
	str := prettifyBytes(1286654845)
	expected := "1.2 GB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_TB(t *testing.T) {
	str := prettifyBytes(1286654845956)
	expected := "1.2 TB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_PB(t *testing.T) {
	str := prettifyBytes(1286654845956265)
	expected := "1.1 PB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Test_PrettifyBytes_EB(t *testing.T) {
	str := prettifyBytes(1286654845956265754)
	expected := "1.1 EB"
	if str != expected {
		t.Errorf("Invalid formatting. Expected: \"%s\", Got: \"%s\"", expected, str)
	}
}

func Benchmark_PrettifyBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		n := i % 7
		switch n {
		case 0:
			prettifyBytes(950) // Bytes
		case 1:
			prettifyBytes(1253) // KB
		case 2:
			prettifyBytes(2531658) // MB
		case 3:
			prettifyBytes(2531658654) // GB
		case 4:
			prettifyBytes(2531658654156) // TB
		case 5:
			prettifyBytes(2531658654156569) // PB
		case 6:
			prettifyBytes(2531658654156569654) // EB
		}
	}
}
