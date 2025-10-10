package provider

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/reconcile-kit/api/resource"
)

// RedisConfig contains configuration for connecting to Redis
type RedisConfig struct {
	Addr        string        // Redis server address (e.g., "localhost:6379")
	Username    string        // Username for authentication
	Password    string        // Password for authentication
	EnableTLS   bool          // Enable system tls certs
	DialTimeout time.Duration // Connection timeout
}

type RedisStreamListener struct {
	rdb             *redis.Client
	group, consumer string
	block           time.Duration // XREADGROUP timeout
	resendInterval  time.Duration // how often to check pending messages
	minIdle         time.Duration // idle time before redelivery
	stream          string
	logger          Logger
}

// NewRedisStreamListener creates a new Redis Stream listener with basic configuration
func NewRedisStreamListener(addr, shardID string) (*RedisStreamListener, error) {
	config := &RedisConfig{
		Addr:        addr,
		DialTimeout: 30 * time.Second,
	}
	return NewRedisStreamListenerWithConfig(config, shardID)
}

// NewRedisStreamListenerWithConfig creates a new Redis Stream listener with full configuration
func NewRedisStreamListenerWithConfig(config *RedisConfig, shardID string, options ...Option) (*RedisStreamListener, error) {
	currentOptions := &opts{}
	for _, opt := range options {
		opt(currentOptions)
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	var logger Logger
	if currentOptions.logger != nil {
		logger = currentOptions.logger
	} else {
		logger = &SimpleLogger{}
	}

	var listenerBlock time.Duration
	if currentOptions.listenerBlock != 0 {
		listenerBlock = currentOptions.listenerBlock
	} else {
		listenerBlock = 0
	}

	var listenerMinIdle time.Duration
	if currentOptions.listenerMinIdle != 0 {
		listenerMinIdle = currentOptions.listenerMinIdle
	} else {
		listenerMinIdle = 60 * time.Second
	}

	var resendInterval time.Duration
	if currentOptions.resendInterval != 0 {
		resendInterval = currentOptions.resendInterval
	} else {
		resendInterval = 3600 * time.Second
	}

	// Configure TLS if certificates are specified
	var tlsConfig *tls.Config
	if config.EnableTLS {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	// Create Redis client with configuration
	rdb := redis.NewClient(&redis.Options{
		Addr:        config.Addr,
		Username:    config.Username,
		Password:    config.Password,
		TLSConfig:   tlsConfig,
		DialTimeout: config.DialTimeout,
	})

	ctx := context.Background()
	stream := shardID + "_stream"
	listener := &RedisStreamListener{
		rdb:            rdb,
		group:          shardID + "_group",
		consumer:       shardID + "_consumer",
		block:          listenerBlock,
		resendInterval: resendInterval,
		minIdle:        listenerMinIdle,
		stream:         stream,
		logger:         logger,
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
			l.logger.Error("redis failed to read from stream: ", err.Error())
			time.Sleep(3 * time.Second)
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
			pending, err := l.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: stream,
				Group:  l.group,
				Start:  "-",
				End:    "+",
				Count:  100,
				Idle:   l.minIdle,
			}).Result()
			if err != nil || len(pending) == 0 {
				if err != nil {
					l.logger.Error("redis failed to get pending messages: ", err.Error())
				}
				continue
			}

			ids := make([]string, len(pending))
			for i, p := range pending {
				ids[i] = p.ID
			}
			messages, err := l.rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   stream,
				Group:    l.group,
				Consumer: l.consumer,
				MinIdle:  l.minIdle,
				Messages: ids,
			}).Result()
			if err != nil {
				l.logger.Error("redis failed to claim messages: ", err.Error())
				continue
			}
			l.handleBatch(ctx, stream, messages, handler)
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
			_, err := l.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.XAck(ctx, stream, l.group, m.ID)
				pipe.XDel(ctx, stream, m.ID)
				return nil
			})
			if err != nil {
				l.logger.Error("redis failed to XAck and XDel message: ", err.Error())
			}
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

// validateConfig validates Redis configuration
func validateConfig(config *RedisConfig) error {
	if config == nil {
		return errors.New("configuration cannot be nil")
	}

	if config.Addr == "" {
		return errors.New("redis server address is required")
	}

	if config.DialTimeout <= 0 {
		config.DialTimeout = 30 * time.Second
	}

	return nil
}
