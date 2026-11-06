package health

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// readProcStat is the Linux source for process CPU. Tests replace it.
var readProcStat = func() ([]byte, error) {
	return os.ReadFile("/proc/self/stat")
}

// clkTck returns the jiffy frequency. Zero means the CPU fields stay omitted.
var clkTck = defaultClkTck

func defaultClkTck() int64 {
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// cpuSampler keeps the previous process tick count. The first call stores
// that point and returns no percent. Later calls compute the delta without
// sleeping.
type cpuSampler struct {
	mu      sync.Mutex
	now     func() time.Time
	read    func() ([]byte, error)
	ticks   func() int64
	has     bool
	jiffies int64
	sampled time.Time
}

func (s *cpuSampler) sample() (percent *float64, sampleMS *int64) {
	if s == nil || runtime.GOOS != "linux" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	read := s.read
	if read == nil {
		read = readProcStat
	}
	ticks := s.ticks
	if ticks == nil {
		ticks = clkTck
	}
	data, err := read()
	if err != nil {
		return nil, nil
	}
	jiffies, ok := parseProcStatCPU(data)
	if !ok {
		return nil, nil
	}
	hz := ticks()
	if !s.has {
		s.has = true
		s.jiffies = jiffies
		s.sampled = now
		return nil, nil
	}
	wall := now.Sub(s.sampled)
	delta := jiffies - s.jiffies
	if wall <= 0 || delta < 0 || hz <= 0 {
		return nil, nil
	}
	pct := 100 * float64(delta) / float64(hz) / wall.Seconds()
	ms := wall.Milliseconds()
	s.jiffies = jiffies
	s.sampled = now
	return &pct, &ms
}

func parseProcStatCPU(data []byte) (int64, bool) {
	text := string(data)
	close := strings.LastIndex(text, ")")
	if close < 0 || close+2 >= len(text) {
		return 0, false
	}
	fields := strings.Fields(text[close+1:])
	// After ")", field 3 (state) is index 0. utime is field 14, stime field 15.
	if len(fields) < 13 {
		return 0, false
	}
	utime, errU := strconv.ParseInt(fields[11], 10, 64)
	stime, errS := strconv.ParseInt(fields[12], 10, 64)
	if errU != nil || errS != nil || utime < 0 || stime < 0 {
		return 0, false
	}
	return utime + stime, true
}

var errNoProcStat = errors.New("proc stat unavailable")

var systemCPUSampler = &cpuSampler{}
