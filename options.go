package provider

import "time"

type opts struct {
	logger          Logger
	listenerBlock   time.Duration
	listenerMinIdle time.Duration
	resendInterval  time.Duration
}

type Option func(opts *opts)

func WithLogger(logger Logger) Option {
	return func(o *opts) {
		o.logger = logger
	}
}

func WithListenerBlock(block time.Duration) Option {
	return func(o *opts) {
		o.listenerBlock = block
	}
}

func WithListenerMinIdle(minIdle time.Duration) Option {
	return func(o *opts) {
		o.listenerMinIdle = minIdle
	}
}

func WithResendInterval(interval time.Duration) Option {
	return func(o *opts) {
		o.resendInterval = interval
	}
}
