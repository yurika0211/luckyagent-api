package health

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const luckyAgentDirCacheTTL = 15 * time.Second

// readProcStatus is the Linux status source for process_rss_bytes.
// Tests replace it. A read error leaves the field omitted.
var readProcStatus = func() ([]byte, error) {
	return os.ReadFile("/proc/self/status")
}

func processRSSBytes() *int64 {
	if runtime.GOOS != "linux" {
		return nil
	}
	data, err := readProcStatus()
	if err != nil {
		return nil
	}
	n, ok := parseVmRSS(data)
	if !ok {
		return nil
	}
	return &n
}

func parseVmRSS(data []byte) (int64, bool) {
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || kb < 0 {
			return 0, false
		}
		return kb * 1024, true
	}
	return 0, false
}

// dirByteCache remembers the last successful directory size. A failed walk
// keeps that value. The TTL is 15s, under the 30s client poll.
type dirByteCache struct {
	mu        sync.Mutex
	path      func() string
	walk      func(root string) (int64, error)
	now       func() time.Time
	has       bool
	value     int64
	root      string
	sampledAt time.Time
}

func (c *dirByteCache) bytes() *int64 {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	root := ""
	if c.path != nil {
		root = c.path()
	}
	if c.has && root == c.root && now.Sub(c.sampledAt) < luckyAgentDirCacheTTL {
		v := c.value
		return &v
	}
	if c.walk == nil || root == "" {
		return c.cachedLocked()
	}
	n, err := c.walk(root)
	if err != nil {
		return c.cachedLocked()
	}
	c.has = true
	c.value = n
	c.root = root
	c.sampledAt = now
	v := n
	return &v
}

func (c *dirByteCache) cachedLocked() *int64 {
	if !c.has {
		return nil
	}
	v := c.value
	return &v
}

func defaultLuckyAgentPath() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return ""
	}
	return filepath.Join(home, ".luckyagent")
}

func walkDirBytes(root string) (int64, error) {
	info, err := os.Stat(root)
	if err != nil {
		return 0, err
	}
	if !info.IsDir() {
		if info.Mode().IsRegular() {
			n := info.Size()
			return n, nil
		}
		return 0, nil
	}
	var total int64
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil || d == nil || d.IsDir() {
			return nil
		}
		inf, infoErr := d.Info()
		if infoErr != nil || !inf.Mode().IsRegular() {
			return nil
		}
		total += inf.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

var systemDirCache = &dirByteCache{
	path: defaultLuckyAgentPath,
	walk: walkDirBytes,
	now:  time.Now,
}
