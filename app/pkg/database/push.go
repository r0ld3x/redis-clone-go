package database

import "log"

func RPushAdd(key string, item string) int {
	val, found := DB.Load(key)
	var slice []string

	if found {
		if s, ok := val.([]string); ok {
			slice = s
		}
	} else {
		slice = []string{}
	}
	slice = append(slice, key)
	log.Println(len(slice))
	DB.Store(key, slice)
	return len(slice)

}
