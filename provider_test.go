package provider

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/reconcile-kit/api/resource"
)

func TestRedisStreamListener(t *testing.T) {

	redisAddr := "localhost:6380"

	/* ── 2. create listener ──────────────────────────────────────── */
	lst, err := NewRedisStreamListener(redisAddr, "test")
	if err != nil {
		t.Fatalf("cannot create redis stream listener: %v", err)
	}

	err = lst.ClearQueue(context.Background())
	if err != nil {
		t.Fatalf("cannot clear queue: %v", err)
	}

	/* ── 3. channel to know that handler has executed ────────────── */
	done := make(chan struct{})

	go func() {
		lst.Listen(func(
			ctx context.Context,
			gk resource.GroupKind,
			key resource.ObjectKey,
			msgType string,
			ack func(),
		) {
			// field checks
			if gk.Group != "core" || gk.Kind != "Widget" {
				t.Errorf("unexpected kind %#v", gk)
			}
			if key.Namespace != "default" || key.Name != "demo" {
				t.Errorf("unexpected key %#v", key)
			}
			if msgType != "update" {
				t.Errorf("unexpected msgType %s", msgType)
			}

			fmt.Println(gk.Kind)

			ack()       // acknowledged
			close(done) // signal to test
		})
	}()

	/* ── 4. serialize event directly to Redis ─────────────────────── */
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: lst.stream,
		Values: map[string]any{
			"resource_group": "core",
			"kind":           "Widget",
			"namespace":      "default",
			"name":           "demo",
			"type":           "update",
		},
	}).Err(); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	/* ── 5. wait for handler to execute (max 2 sec) ─────────────── */
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called in time")
	}

	/* ── 6. ensure pending list is empty after ack ───────────── */
	p, err := rdb.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: lst.stream,
		Group:  lst.group,
		Start:  "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if len(p) != 0 {
		for _, ext := range p {
			fmt.Println(ext.Consumer)
		}
		t.Errorf("unexpected pending messages: %#v", p)
	}
}

func TestRedisStreamListenerWithAuth(t *testing.T) {
	redisAddr := "redis:6000"

	// Test with authentication
	config := &RedisConfig{
		Addr:        redisAddr,
		Username:    "default",
		Password:    "password",
		DialTimeout: 30 * time.Second,
	}

	lst, err := NewRedisStreamListenerWithConfig(config, "test_auth")
	if err != nil {
		t.Fatalf("cannot create redis stream listener with auth: %v", err)
	}

	// Clear queue
	err = lst.ClearQueue(context.Background())
	if err != nil {
		t.Fatalf("cannot clear queue: %v", err)
	}

	// Test message sending
	done := make(chan struct{})

	go func() {
		lst.Listen(func(
			ctx context.Context,
			gk resource.GroupKind,
			key resource.ObjectKey,
			msgType string,
			ack func(),
		) {
			if gk.Group != "core" || gk.Kind != "Widget" {
				t.Errorf("unexpected kind %#v", gk)
			}
			if key.Namespace != "default" || key.Name != "demo" {
				t.Errorf("unexpected key %#v", key)
			}
			if msgType != "update" {
				t.Errorf("unexpected msgType %s", msgType)
			}

			ack()
			close(done)
		})
	}()

	// Configure TLS if certificates are specified
	var tlsConfig *tls.Config
	if config.EnableTLS {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Create client with the same authentication settings
	rdb := redis.NewClient(&redis.Options{
		Addr:      redisAddr,
		Username:  "state_manager",
		Password:  "1111",
		TLSConfig: tlsConfig,
	})

	if err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: lst.stream,
		Values: map[string]any{
			"resource_group": "core",
			"kind":           "Widget",
			"namespace":      "default",
			"name":           "demo",
			"type":           "update",
		},
	}).Err(); err != nil {
		t.Fatalf("XAdd failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called in time")
	}
}
