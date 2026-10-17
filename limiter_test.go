package transport

import (
	"testing"
	"time"
)

// TestCleanup 测试 Limiter.cleanup 函数
func TestCleanup(t *testing.T) {
	rl := &Limiter{
		clients: make(map[string]*clientBucket),
		limit:   10,
	}

	// 添加一个已过期的客户端
	expiredIP := "192.168.1.1"
	rl.clients[expiredIP] = &clientBucket{
		count:   5,
		resetAt: time.Now().Add(-2 * time.Minute), // 已过期
	}

	// 添加一个未过期的客户端
	validIP := "192.168.1.2"
	rl.clients[validIP] = &clientBucket{
		count:   3,
		resetAt: time.Now().Add(2 * time.Minute), // 未过期
	}

	// 执行清理
	rl.cleanup()

	// 验证过期的客户端被删除
	if _, exists := rl.clients[expiredIP]; exists {
		t.Error("cleanup: expected expired client to be removed")
	}

	// 验证未过期的客户端保留
	if _, exists := rl.clients[validIP]; !exists {
		t.Error("cleanup: expected valid client to be retained")
	}
}

// TestRateLimiterAllow 测试 Limiter.Allow 函数
func TestRateLimiterAllow(t *testing.T) {
	rl := &Limiter{
		clients: make(map[string]*clientBucket),
		limit:   5,
	}

	ip := "192.168.1.1"

	// 测试限流逻辑
	for i := 0; i < 10; i++ {
		allowed := rl.Allow(ip)
		if i < 5 {
			if !allowed {
				t.Errorf("Allow: request %d should be allowed", i+1)
			}
		} else {
			if allowed {
				t.Errorf("Allow: request %d should be rate limited", i+1)
			}
		}
	}
}
