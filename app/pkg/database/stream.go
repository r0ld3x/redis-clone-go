package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func GetOrCreateStream(key string) *Stream {

	val, exists := DB.Load(key)

	if !exists {
		stream := &Stream{
			Entries:    make([]StreamEntry, 0),
			LastID:     "0-0",
			LastSeqNum: 0,
		}

		streamData := StreamData{
			Stream: stream,
			Px:     -1,
			T:      time.Now(),
		}
		DB.Store(key, streamData)
		return stream
	}
	streamData, ok := val.(StreamData)
	if !ok {
		return nil
	}
	if streamData.Px != -1 && time.Now().After(streamData.T.Add(time.Duration(streamData.Px)*time.Millisecond)) {
		stream := &Stream{
			Entries:    make([]StreamEntry, 0),
			LastID:     "0-0",
			LastSeqNum: 0,
		}
		newStreamData := StreamData{
			Stream: stream,
			Px:     -1,
			T:      time.Now(),
		}
		DB.Store(key, newStreamData)
		return stream
	}
	return streamData.Stream

}

func StreamAdd(key, id string, fields []string) (string, error) {

	if len(fields)%2 != 0 {
		return "", fmt.Errorf("ERR wrong number of arguments for XADD")
	}
	stream := GetOrCreateStream(key)
	if stream == nil {
		return "", fmt.Errorf("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	entryID, err := generateStreamID(stream, id)
	if err != nil {
		return "", err
	}
	fieldMap := make(map[string]string)
	for i := 0; i < len(fields); i += 2 {
		fieldMap[fields[i]] = fields[i+1]
	}

	entry := StreamEntry{
		ID:     entryID,
		Fields: fieldMap,
		Time:   time.Now(),
	}
	stream.Entries = append(stream.Entries, entry)
	stream.LastID = entryID
	parts := strings.Split(entryID, "-")
	if len(parts) == 2 {
		stream.LastSeqNum, _ = strconv.ParseInt(parts[1], 10, 64)
	}
	return entryID, nil

}

func generateStreamID(stream *Stream, requestedID string) (string, error) {
	fmt.Printf("stream %+v, requestedID %+v, ", stream, requestedID)
	now := time.Now()
	currentMs := now.UnixMilli()

	if requestedID == "*" {
		// Auto-generate full ID
		if stream.LastSeqNum == 0 {
			return fmt.Sprintf("%d-0", currentMs), nil
		}

		parts := strings.Split(stream.LastID, "-")
		lastMs, _ := strconv.ParseInt(parts[0], 10, 64)
		lastSeq, _ := strconv.ParseInt(parts[1], 10, 64)
		fmt.Printf("lastMS %+v, lastSeq %+v, ", lastMs, lastSeq)

		if currentMs > lastMs {
			return fmt.Sprintf("%d-0", currentMs), nil
		} else if currentMs == lastMs {
			return fmt.Sprintf("%d-%d", currentMs, lastSeq+1), nil
		} else {
			return fmt.Sprintf("%d-%d", lastMs, lastSeq+1), nil
		}
	}

	if strings.HasSuffix(requestedID, "-*") {
		timestampPart := strings.TrimSuffix(requestedID, "-*")

		requestedMs, err := strconv.ParseInt(timestampPart, 10, 64)
		if err != nil {
			return "", fmt.Errorf("ERR Invalid stream ID format")
		}

		if requestedMs == 0 {
			if len(stream.Entries) == 0 {
				return "0-1", nil
			}

			parts := strings.Split(stream.LastID, "-")
			lastMs, _ := strconv.ParseInt(parts[0], 10, 64)
			lastSeq, _ := strconv.ParseInt(parts[1], 10, 64)

			if lastMs == 0 {
				return fmt.Sprintf("0-%d", lastSeq+1), nil
			} else if lastMs > 0 {
				return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}

		if len(stream.Entries) == 0 {
			return fmt.Sprintf("%s-0", timestampPart), nil
		}

		// Parse last ID
		parts := strings.Split(stream.LastID, "-")
		lastMs, _ := strconv.ParseInt(parts[0], 10, 64)
		lastSeq, _ := strconv.ParseInt(parts[1], 10, 64)

		if requestedMs > lastMs {
			return fmt.Sprintf("%s-0", timestampPart), nil
		} else if requestedMs == lastMs {
			return fmt.Sprintf("%s-%d", timestampPart, lastSeq+1), nil
		} else {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	isValid, err := isValidStreamID(stream, requestedID)
	if !isValid {
		return "", err
	}

	return requestedID, nil
}

func isValidStreamID(stream *Stream, id string) (bool, error) {
	if id == "0-0" && len(stream.Entries) == 0 {
		return false, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	if len(stream.Entries) == 0 {
		if id == "0-0" {
			return false, fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
		}
		return true, nil
	}

	isValid := compareStreamIDs(id, stream.LastID) > 0
	if !isValid {
		return false, fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return true, nil
}
func compareStreamIDs(id1, id2 string) int {
	parts1 := strings.Split(id1, "-")
	parts2 := strings.Split(id2, "-")

	if len(parts1) != 2 || len(parts2) != 2 {
		return 0
	}

	ms1, _ := strconv.ParseInt(parts1[0], 10, 64)
	seq1, _ := strconv.ParseInt(parts1[1], 10, 64)
	ms2, _ := strconv.ParseInt(parts2[0], 10, 64)
	seq2, _ := strconv.ParseInt(parts2[1], 10, 64)

	// Compare milliseconds first
	if ms1 != ms2 {
		if ms1 > ms2 {
			return 1
		}
		return -1
	}

	// If milliseconds are equal, compare sequence numbers
	if seq1 > seq2 {
		return 1
	} else if seq1 < seq2 {
		return -1
	}
	return 0
}

func StreamRange(key, start, end string) ([]StreamEntry, error) {

	val, exists := DB.Load(key)
	if !exists {
		return []StreamEntry{}, nil
	}
	streamData, ok := val.(StreamData)
	if !ok {
		return nil, fmt.Errorf("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if streamData.Px != -1 && time.Now().After(streamData.T.Add(time.Millisecond*time.Duration(streamData.Px))) {
		return []StreamEntry{}, nil
	}
	stream := streamData.Stream
	stream.mutex.RLock()
	defer stream.mutex.RUnlock()
	var result []StreamEntry
	for _, entry := range stream.Entries {
		if (start == "-" || compareStreamIDs(entry.ID, start) >= 0) &&
			(end == "+" || compareStreamIDs(entry.ID, end) <= 0) {
			result = append(result, entry)
		}
	}
	return result, nil

}

func StreamReadFrom(key, startID string) ([]StreamEntry, error) {
	val, exists := DB.Load(key)
	if !exists {
		return []StreamEntry{}, nil
	}

	streamData, ok := val.(StreamData)
	if !ok {
		return nil, fmt.Errorf("ERR WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Check expiration
	if streamData.Px != -1 && time.Now().After(streamData.T.Add(time.Millisecond*time.Duration(streamData.Px))) {
		return []StreamEntry{}, nil
	}

	stream := streamData.Stream
	stream.mutex.RLock()
	defer stream.mutex.RUnlock()

	var result []StreamEntry

	// Handle special cases
	if startID == "$" {
		// $ means "latest ID" - return empty for non-blocking reads
		return []StreamEntry{}, nil
	}

	for _, entry := range stream.Entries {
		// For XREAD, we want entries AFTER the specified ID
		if compareStreamIDs(entry.ID, startID) > 0 {
			result = append(result, entry)
		}
	}

	return result, nil
}

// StreamReadMultiple reads from multiple streams
func StreamReadMultiple(streamKeys []string, startIDs []string) (map[string][]StreamEntry, error) {
	if len(streamKeys) != len(startIDs) {
		return nil, fmt.Errorf("ERR number of keys must match number of IDs")
	}

	results := make(map[string][]StreamEntry)

	for i, key := range streamKeys {
		entries, err := StreamReadFrom(key, startIDs[i])
		if err != nil {
			return nil, err
		}

		// Only include streams that have entries
		if len(entries) > 0 {
			results[key] = entries
		}
	}

	return results, nil
}

// GetStreamLastID returns the last ID of a stream, or "0-0" if stream doesn't exist
func GetStreamLastID(key string) string {
	val, exists := DB.Load(key)
	if !exists {
		return "0-0"
	}

	streamData, ok := val.(StreamData)
	if !ok {
		return "0-0"
	}

	// Check expiration
	if streamData.Px != -1 && time.Now().After(streamData.T.Add(time.Millisecond*time.Duration(streamData.Px))) {
		return "0-0"
	}

	return streamData.Stream.LastID
}
