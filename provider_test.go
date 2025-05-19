package provider

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/reconcile-kit/api/resource"
)

func TestRedisStreamListener(t *testing.T) {

	redisAddr := "localhost:6380"

	/* ── 2. создаём listener ──────────────────────────────────────── */
	lst, err := NewRedisStreamListener(redisAddr, "test")
	if err != nil {
		t.Fatalf("cannot create redis stream listener: %v", err)
	}

	err = lst.ClearQueue(context.Background())
	if err != nil {
		t.Fatalf("cannot clear queue: %v", err)
	}

	/* ── 3. канал, чтобы узнать, что handler отработал ────────────── */
	done := make(chan struct{})

	go func() {
		lst.Listen(func(
			ctx context.Context,
			gk resource.GroupKind,
			key resource.ObjectKey,
			msgType string,
			ack func(),
		) {
			// проверки полей
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

			ack()       // подтвердили
			close(done) // сигнал тесту
		})
	}()

	/* ── 4. сериализуем событие прямо в Redis ─────────────────────── */
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

	/* ── 5. ждём, чтобы handler отработал (max 2 сек) ─────────────── */
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not called in time")
	}

	/* ── 6. убеждаемся, что pending-лист пуст после ack ───────────── */
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
