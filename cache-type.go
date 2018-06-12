package glib

import "time"

type Cacher interface {
	// update value's expire time to timeout
	Touch(key string, timeout time.Duration) error
	// Get cached value by key.
	Get(key string) (interface{}, error)
	// Put cached value with key and expire time.
	Put(key string, val interface{}, timeout time.Duration) error
	// Increment cached int value by key, as a counter.
	Increment(key string) error
	// Decrement cached int value by key, as a counter.
	Decrement(key string) error
	// Delete cached value by key.
	Delete(key string) error
	// Clear all cache.
	ClearAll() error
}

type CacheCreator func(config *CacheConfig) Cacher

type CacheConfig struct {
	Enable bool          `json:"enable"`
	Alias  string        `json:"alias"`
	Driver string        `json:"driver"`
	Dsn    string        `json:"dsn"`
	TTL    time.Duration `json:"ttl"`
}
