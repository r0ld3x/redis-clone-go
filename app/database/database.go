package database

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var DB sync.Map

func Start() {
	sync.OnceFunc(func() {
		DB = sync.Map{}
	})
}

type KeyValue struct {
	Val string
	Px  int
	T   time.Time
}

type StreamEntry struct {
	ID     string
	Fields map[string]string
	Time   time.Time
}

type Stream struct {
	Entries    []StreamEntry
	LastID     string
	LastSeqNum int64
	mutex      sync.RWMutex
}

type StreamData struct {
	Stream *Stream
	Px     int
	T      time.Time
}

func SetKey(key, val string, px int) {
	data := KeyValue{
		Val: val,
		T:   time.Now(),
		Px:  px,
	}

	DB.Store(key, data)
	fmt.Printf("key: %+v\n", key)

}

func GetKey(key string) (string, bool) {
	val, found := DB.Load(key)
	if !found {
		return "", false
	}
	data, ok := val.(KeyValue)
	if !ok {
		return "", false
	}
	if data.Px != -1 &&
		time.Now().After(data.T.Add(time.Millisecond*time.Duration(data.Px))) {
		return "", false
	}
	return data.Val, true

}

func GetType(key string) (string, bool) {
	val, found := DB.Load(key)
	fmt.Printf("key: %s, val: %+v, found: %t\n", key, val, found)
	if !found {
		return "", false
	}
	switch v := val.(type) {
	case KeyValue:
		if v.Px != -1 && time.Now().After(v.T.Add(time.Millisecond*time.Duration(v.Px))) {
			return "", false
		}
		if _, err := strconv.Atoi(v.Val); err == nil {
			return "integer", true
		}

		if _, err := strconv.ParseFloat(v.Val, 64); err == nil {
			return "float", true
		}

		return "string", true
	case StreamData:
		if v.Px != -1 && time.Now().After(v.T.Add(time.Millisecond*time.Duration(v.Px))) {
			return "", false
		}
		return "stream", true
	default:
		return "", false
	}

}

func DeleteKey(key string) {
	DB.Delete(key)
}

func Increment(key string, by int) (string, bool) {
	val, found := DB.Load(key)
	if !found {
		data := KeyValue{
			Val: strconv.Itoa(by),
			Px:  -1,
			T:   time.Now(),
		}
		DB.Store(key, data)
		return data.Val, true
	}
	data, ok := val.(KeyValue)
	if !ok {
		return "", false
	}
	if data.Px != -1 && time.Now().After(data.T.Add(time.Duration(data.Px)*time.Millisecond)) {
		data := KeyValue{
			Val: strconv.Itoa(by),
			Px:  -1,
			T:   time.Now(),
		}
		DB.Store(key, data)
		return data.Val, true
	}
	currentInt, err := strconv.Atoi(data.Val)
	if err != nil {
		return "", false
	}
	newVal := currentInt + by
	data.Val = strconv.Itoa(newVal)
	data.T = time.Now()
	DB.Store(key, data)
	return data.Val, true

}
