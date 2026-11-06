package health

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseProcStatCPUCommWithSpaces(t *testing.T) {
	raw := []byte("123 (my proc name) R 1 2 3 4 5 6 7 8 9 10 40 10 0\n")
	got, ok := parseProcStatCPU(raw)
	if !ok || got != 50 {
		t.Fatalf("parse = %d ok=%v", got, ok)
	}
	if _, ok := parseProcStatCPU([]byte("123 (no close\n")); ok {
		t.Fatal("missing close paren should fail")
	}
	if _, ok := parseProcStatCPU([]byte("123 (name) R 1\n")); ok {
		t.Fatal("short stat should fail")
	}
}

func TestCPUFirstSampleOmitsJSON(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	sampler := &cpuSampler{
		now:   func() time.Time { return now },
		read:  func() ([]byte, error) { return []byte("1 (lh) S 0 0 0 0 0 0 0 0 0 0 10 0\n"), nil },
		ticks: func() int64 { return 100 },
	}
	prev := systemCPUSampler
	systemCPUSampler = sampler
	t.Cleanup(func() { systemCPUSampler = prev })

	info := getSystemInfo()
	if info.ProcessCPUPercent != nil || info.ProcessCPUSampleMS != nil {
		t.Fatalf("first sample should omit CPU, got %#v %#v", info.ProcessCPUPercent, info.ProcessCPUSampleMS)
	}
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "process_cpu_percent") || strings.Contains(text, "process_cpu_sample_ms") {
		t.Fatalf("first sample JSON leaked CPU: %s", text)
	}
}

func TestCPUSecondSampleMatchesFormula(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	jiffies := int64(10)
	sampler := &cpuSampler{
		now: func() time.Time { return now },
		read: func() ([]byte, error) {
			return []byte("7 (lucky agent) R 1 1 1 0 0 0 0 0 0 0 " + itoa(jiffies) + " 0\n"), nil
		},
		ticks: func() int64 { return 100 },
	}
	prev := systemCPUSampler
	systemCPUSampler = sampler
	t.Cleanup(func() { systemCPUSampler = prev })

	if pct, ms := sampler.sample(); pct != nil || ms != nil {
		t.Fatalf("first = %#v %#v", pct, ms)
	}
	now = now.Add(time.Second)
	jiffies = 60
	pct, ms := sampler.sample()
	if pct == nil || *pct != 50 || ms == nil || *ms != 1000 {
		t.Fatalf("percent=%v sample_ms=%v", pct, ms)
	}
}

func TestCPUJSONOmitsNilAndShowsSetValues(t *testing.T) {
	data, err := json.Marshal(SystemInfo{GoVersion: "go1.22", NumCPU: 1})
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "process_cpu_percent") || strings.Contains(text, "process_cpu_sample_ms") {
		t.Fatalf("nil CPU pointers should be omitted: %s", text)
	}
	pct := 50.0
	ms := int64(1000)
	data, err = json.Marshal(SystemInfo{
		GoVersion:          "go1.22",
		NumCPU:             1,
		ProcessCPUPercent:  &pct,
		ProcessCPUSampleMS: &ms,
	})
	if err != nil {
		t.Fatal(err)
	}
	text = string(data)
	if !strings.Contains(text, `"process_cpu_percent":50`) || !strings.Contains(text, `"process_cpu_sample_ms":1000`) {
		t.Fatalf("set CPU pointers: %s", text)
	}
}

func TestCPUBadDeltaOmitsAndKeepsPreviousPoint(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)
	sampler := &cpuSampler{
		now:     func() time.Time { return now },
		read:    func() ([]byte, error) { return []byte("1 (lh) S 0 0 0 0 0 0 0 0 0 0 20 5\n"), nil },
		ticks:   func() int64 { return 100 },
		has:     true,
		jiffies: 100,
		sampled: now.Add(-time.Second),
	}
	pct, ms := sampler.sample()
	if pct != nil || ms != nil {
		t.Fatalf("negative delta should omit, got %v %v", pct, ms)
	}
	if !sampler.has || sampler.jiffies != 100 {
		t.Fatalf("previous point changed: has=%v jiffies=%d", sampler.has, sampler.jiffies)
	}
}

func itoa(n int64) string {
	return strconvItoa(n)
}

func strconvItoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [32]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
