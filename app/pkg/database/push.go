package database

import (
	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
)

func RPushAdd(key string, item string) int {
	logger := logging.NewLogger("RPUSH")

	val, found := DB.Load(key)
	var slice []string

	if found {
		if s, ok := val.([]string); ok {
			slice = s
		} else {
			// If the existing value is not a slice, create a new slice
			slice = []string{}
		}
	} else {
		slice = []string{}
	}

	slice = append(slice, item)
	DB.Store(key, slice)

	logger.Debug("RPUSH: Added item '%s' to key '%s', new length: %d", item, key, len(slice))
	return len(slice)
}
