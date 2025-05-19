package provider

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"

	"github.com/reconcile-kit/api/resource"
)

type RedisStreamListener struct {
	rdb             *redis.Client
	group, consumer string
	block           time.Duration // timeout XREADGROUP
	resendInterval  time.Duration // как часто проверять pending
	minIdle         time.Duration // «просрочка» для повторной доставки
	stream          string
}

func NewRedisStreamListener(addr, shardID string) (*RedisStreamListener, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 30 * time.Second})
	ctx := context.Background()
	stream := shardID + "_stream"
	listener := &RedisStreamListener{
		rdb:            rdb,
		group:          shardID + "_group",
		consumer:       shardID + "_consumer",
		block:          5 * time.Second,
		resendInterval: 60 * time.Second,
		minIdle:        50 * time.Second,
		stream:         stream,
	}

	if err := listener.ensureGroup(ctx, stream); err != nil {
		return nil, err
	}

	return listener, nil
}

func (l *RedisStreamListener) Listen(
	handler func(ctx context.Context,
		kind resource.GroupKind,
		key resource.ObjectKey,
		msgType string,
		ack func()),
) {
	ctx := context.Background()
	go l.resendLoop(ctx, l.stream, handler)

	for {
		recs, err := l.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    l.group,
			Consumer: l.consumer,
			Streams:  []string{l.stream, ">"},
			Count:    10,
			Block:    l.block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		l.handleBatch(ctx, l.stream, recs[0].Messages, handler)
	}
}

func (l *RedisStreamListener) ClearQueue(ctx context.Context) error {
	iter := l.rdb.Scan(ctx, 0, l.stream, 0).Iterator()
	for iter.Next(ctx) {
		stream := iter.Val()

		if err := l.rdb.Do(ctx, "XTRIM", stream, "MAXLEN", "0").Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}
func (l *RedisStreamListener) resendLoop(
	ctx context.Context,
	stream string,
	handler func(ctx context.Context, kind resource.GroupKind, key resource.ObjectKey, msgType string, ack func()),
) {
	tick := time.NewTicker(l.resendInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			// 1. получаем список «старых» pending-сообщений
			pending, err := l.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: stream,
				Group:  l.group,
				Start:  "-",
				End:    "+",
				Count:  100,
				Idle:   l.minIdle,
			}).Result()
			if err != nil || len(pending) == 0 {
				continue
			}

			ids := make([]string, len(pending))
			for i, p := range pending {
				ids[i] = p.ID
			}
			msgs, err := l.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   stream,
				Group:    l.group,
				Consumer: l.consumer,
				MinIdle:  l.minIdle,
				Messages: ids,
			}).Result()
			if err != nil {
				continue
			}
			l.handleBatch(ctx, stream, msgs, handler)
		}
	}
}

func (l *RedisStreamListener) handleBatch(
	ctx context.Context,
	stream string,
	msgs []redis.XMessage,
	handler func(ctx context.Context, kind resource.GroupKind, key resource.ObjectKey, msgType string, ack func()),
) {
	for _, m := range msgs {

		kind := resource.GroupKind{
			Group: m.Values["resource_group"].(string),
			Kind:  m.Values["kind"].(string),
		}
		key := resource.ObjectKey{
			Namespace: m.Values["namespace"].(string),
			Name:      m.Values["name"].(string),
		}
		evType := m.Values["type"].(string)

		ack := func() {
			_ = l.rdb.XAck(ctx, stream, l.group, m.ID).Err()
		}
		handler(ctx, kind, key, evType, ack)
	}
}

func (l *RedisStreamListener) ensureGroup(ctx context.Context, stream string) error {
	if err := l.rdb.XGroupCreateMkStream(ctx, stream, l.group, "0").Err(); err != nil &&
		!strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}
