package health

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "health-luckyagent")
	prevPath := systemDirCache.path
	if err == nil {
		systemDirCache.path = func() string { return dir }
	}
	code := m.Run()
	systemDirCache.path = prevPath
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
	os.Exit(code)
}

func TestParseVmRSS(t *testing.T) {
	n, ok := parseVmRSS([]byte("Name:\tproc\nVmRSS:      145000 kB\nVmSize:\t1 kB\n"))
	if !ok || n != 145000*1024 {
		t.Fatalf("parse = %d ok=%v", n, ok)
	}
	if _, ok := parseVmRSS([]byte("Name:\tproc\nVmSize:\t1 kB\n")); ok {
		t.Fatal("missing VmRSS should fail")
	}
	if _, ok := parseVmRSS([]byte("VmRSS:      nope kB\n")); ok {
		t.Fatal("garbage VmRSS should fail")
	}
	prev := readProcStatus
	readProcStatus = func() ([]byte, error) { return nil, errors.New("unreadable") }
	t.Cleanup(func() { readProcStatus = prev })
	if processRSSBytes() != nil {
		t.Fatal("unreadable status should omit process_rss_bytes")
	}
}

func TestWalkDirBytesExactAndMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.txt"), []byte("de"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := walkDirBytes(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("dir bytes = %d, want 5", got)
	}
	if _, err := walkDirBytes(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing dir should fail the walk")
	}
	cache := &dirByteCache{
		path: func() string { return filepath.Join(dir, "missing") },
		walk: walkDirBytes,
		now:  time.Now,
	}
	if cache.bytes() != nil {
		t.Fatal("missing dir should omit luckyagent_dir_bytes")
	}
}

func TestSystemInfoJSONOmitsNilResourceFields(t *testing.T) {
	data, err := json.Marshal(SystemInfo{GoVersion: "go1.22", NumCPU: 1})
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if strings.Contains(text, "process_rss_bytes") || strings.Contains(text, "luckyagent_dir_bytes") {
		t.Fatalf("nil pointers should be omitted: %s", text)
	}
	rss := int64(145000 * 1024)
	dir := int64(5)
	data, err = json.Marshal(SystemInfo{
		GoVersion:          "go1.22",
		NumCPU:             1,
		ProcessRSSBytes:    &rss,
		LuckyAgentDirBytes: &dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	text = string(data)
	if !strings.Contains(text, `"process_rss_bytes":148480000`) || !strings.Contains(text, `"luckyagent_dir_bytes":5`) {
		t.Fatalf("set pointers should be raw bytes: %s", text)
	}
	if !strings.Contains(text, `"heap_alloc_mb"`) {
		t.Fatal("heap field name changed")
	}
}

func TestDirBytesCacheTTLAndFailure(t *testing.T) {
	var calls int
	now := time.Unix(1_700_000_000, 0)
	cache := &dirByteCache{
		path: func() string { return "/virtual/.luckyagent" },
		walk: func(root string) (int64, error) {
			calls++
			if calls >= 3 {
				return 0, errors.New("stat failed")
			}
			return int64(100 + calls), nil
		},
		now: func() time.Time { return now },
	}
	prev := systemDirCache
	systemDirCache = cache
	t.Cleanup(func() { systemDirCache = prev })

	first := getSystemInfo()
	second := getSystemInfo()
	if calls != 1 {
		t.Fatalf("walks inside TTL = %d, want 1", calls)
	}
	if first.LuckyAgentDirBytes == nil || *first.LuckyAgentDirBytes != 101 {
		t.Fatalf("first = %#v", first.LuckyAgentDirBytes)
	}
	if second.LuckyAgentDirBytes == nil || *second.LuckyAgentDirBytes != 101 {
		t.Fatalf("second = %#v", second.LuckyAgentDirBytes)
	}

	now = now.Add(luckyAgentDirCacheTTL)
	third := getSystemInfo()
	if calls != 2 {
		t.Fatalf("walks after TTL = %d, want 2", calls)
	}
	if third.LuckyAgentDirBytes == nil || *third.LuckyAgentDirBytes != 102 {
		t.Fatalf("third = %#v", third.LuckyAgentDirBytes)
	}

	now = now.Add(luckyAgentDirCacheTTL)
	fourth := getSystemInfo()
	if calls != 3 {
		t.Fatalf("walks on failure = %d, want 3", calls)
	}
	if fourth.LuckyAgentDirBytes == nil || *fourth.LuckyAgentDirBytes != 102 {
		t.Fatalf("failure should keep last success, got %#v", fourth.LuckyAgentDirBytes)
	}
	if first.HeapAllocMB <= 0 {
		t.Fatal("heap_alloc_mb should stay a positive MB value")
	}
}
