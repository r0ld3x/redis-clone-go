package database

import (
	"errors"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
)

func RPushAdd(key string, item string) (int, error) {
	logger := logging.NewLogger("RPUSH")

	val, found := DB.Load(key)
	var slice []string

	if found {
		if s, ok := val.([]string); ok {
			slice = s
		} else {
			return 0, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	} else {
		slice = []string{}
	}

	slice = append(slice, item)
	DB.Store(key, slice)

	logger.Debug("RPUSH: Added item '%s' to key '%s', new length: %d", item, key, len(slice))
	return len(slice), nil
}

func LRangeAdd(key string, start int, end int) ([]string, error) {
	// logger := logging.NewLogger("LRANGE")

	val, found := DB.Load(key)
	var slice []string

	if found {
		if s, ok := val.([]string); ok {
			slice = s
		} else {
			return nil, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	} else {
		return []string{}, nil
	}
	length := len(slice)

	if start < 0 {
		test := length + start
		if test < 0 {
			start = 0
		} else {
			start = test
		}
	}

	if end < 0 {
		test := length + end
		if test < 0 {
			end = 0
		} else {
			end = test
		}
	}

	if start >= length || start > end {
		return []string{}, nil
	}
	if end >= length {
		end = length - 1
	}
	return slice[start : end+1], nil
}
