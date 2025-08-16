package database

import (
	"errors"
	"fmt"

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

func LRange(key string, start int, end int) ([]string, error) {
	logger := logging.NewLogger("LRANGE")

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
	logger.Info("slice: %+v", slice)
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

func LPush(key string, values string) (int, error) {
	logger := logging.NewLogger("LPUSH")

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

	slice = append([]string{values}, slice...)

	DB.Store(key, slice)

	logger.Debug("LPUSH: Added item '%+v' to key '%s', new length: %d", values, key, len(slice))
	return len(slice), nil
}

func GetArrayLength(key string) (int, error) {

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

	return len(slice), nil
}

func RemoveNFromArray(key string, n int) ([]string, error) {

	val, found := DB.Load(key)
	var slice []string

	if found {
		if s, ok := val.([]string); ok {
			slice = s
		} else {
			return []string{}, errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	} else {
		slice = []string{}
	}

	length := len(slice)
	if length == 0 {
		return []string{}, nil
	}

	toRemove := n + 1

	if toRemove > length {
		DB.Store(key, []string{})
		return slice, nil
	}

	fmt.Printf("len(slice): %d, toRemove: %d\n", length, toRemove)
	fmt.Printf("slice[:toRemove]: %+v\n", slice[:toRemove])
	fmt.Printf("slice[toRemove:]: %+v\n", slice[toRemove:])

	removedItems := slice[:toRemove]
	remaining := slice[toRemove:]

	DB.Store(key, remaining)

	return removedItems, nil
}
