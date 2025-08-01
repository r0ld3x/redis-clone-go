package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"
	"github.com/r0ld3x/redis-clone-go/app/internal/server"

	"github.com/r0ld3x/redis-clone-go/app/pkg/database"
)

// XAddHandler handles XADD commands
type XAddHandler struct {
	logger *logging.Logger
}

func (h *XAddHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("XADD")
	}

	if len(args) < 3 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'XADD'")
		return nil
	}

	key := args[0]
	id := args[1]
	if id == "0-0" {
		protocol.WriteError(clientConn, "ERR The ID specified in XADD must be greater than 0-0")
		return nil
	}
	fields := args[2:]

	entryID, err := database.StreamAdd(key, id, fields)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return nil
	}

	protocol.WriteBulkString(clientConn, entryID)
	return nil
}

// XRangeHandler handles XRANGE commands
type XRangeHandler struct {
	logger *logging.Logger
}

func (h *XRangeHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("XRANGE")
	}

	if len(args) < 3 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'XRANGE'")
		return nil
	}

	key := args[0]
	start := args[1]
	end := args[2]

	entries, err := database.StreamRange(key, start, end)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return nil
	}

	respData := h.formatStreamEntries(entries)
	clientConn.Write([]byte(respData))
	fmt.Printf("respData: %q\n", respData)
	return nil
}

func (h *XRangeHandler) formatStreamEntries(entries []database.StreamEntry) string {
	// Start with array header
	response := fmt.Sprintf("*%d\r\n", len(entries))

	for _, entry := range entries {
		// Each entry is an array with 2 elements: [ID, [field1, value1, field2, value2, ...]]
		response += "*2\r\n"

		// 1. Entry ID as bulk string
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID)

		// 2. Fields array
		fieldCount := len(entry.Fields) * 2 // Each field has name and value
		response += fmt.Sprintf("*%d\r\n", fieldCount)

		// Add each field name and value as bulk strings
		for fieldName, fieldValue := range entry.Fields {
			// Field name
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(fieldName), fieldName)
			// Field value
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(fieldValue), fieldValue)
		}
	}

	return response
}

// XReadHandler handles XREAD commands
type XReadHandler struct {
	logger *logging.Logger
}

func (h *XReadHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("XREAD")
	}

	if len(args) < 3 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'XREAD'")
		return nil
	}

	var blockTimeout int64 = -1
	var argIndex = 0

	if strings.ToUpper(args[0]) == "BLOCK" {
		if len(args) < 5 {
			protocol.WriteError(clientConn, "ERR wrong number of arguments for 'XREAD'")
			return nil
		}

		var err error
		blockTimeout, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			protocol.WriteError(clientConn, "ERR timeout is not an integer or out of range")
			return nil
		}
		argIndex = 2
	}

	if strings.ToUpper(args[argIndex]) != "STREAMS" {
		protocol.WriteError(clientConn, "ERR syntax error")
		return nil
	}
	argIndex++

	// Parse streams and IDs
	remainingArgs := args[argIndex:]
	if len(remainingArgs)%2 != 0 {
		protocol.WriteError(clientConn, "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified")
		return nil
	}

	streamCount := len(remainingArgs) / 2
	streamKeys := remainingArgs[:streamCount]
	startIDs := remainingArgs[streamCount:]

	// Handle '$' IDs - replace with actual last IDs
	for i, id := range startIDs {
		if id == "$" {
			startIDs[i] = database.GetStreamLastID(streamKeys[i])
		}
	}

	// Perform the read operation
	if blockTimeout == -1 {
		// Non-blocking read
		h.performRead(srv, clientConn, streamKeys, startIDs)
	} else {
		// Blocking read
		h.performBlockingRead(srv, clientConn, streamKeys, startIDs, blockTimeout)
	}

	return nil
}

func (h *XReadHandler) performRead(srv *server.Server, clientConn net.Conn, streamKeys []string, startIDs []string) {
	results, err := database.StreamReadMultiple(streamKeys, startIDs)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return
	}

	// Format response
	h.writeXreadResponse(clientConn, results, streamKeys)
}

func (h *XReadHandler) performBlockingRead(srv *server.Server, clientConn net.Conn, streamKeys []string, startIDs []string, blockTimeout int64) {
	startTime := time.Now()

	for {
		// Try to read
		results, err := database.StreamReadMultiple(streamKeys, startIDs)
		if err != nil {
			protocol.WriteError(clientConn, err.Error())
			return
		}

		// If we have results, return them
		if len(results) > 0 {
			h.writeXreadResponse(clientConn, results, streamKeys)
			return
		}

		// Check timeout
		if blockTimeout > 0 {
			elapsed := time.Since(startTime).Milliseconds()
			if elapsed >= blockTimeout {
				// Timeout reached, return empty result
				clientConn.Write([]byte("$-1\r\n")) // null response
				return
			}
		}

		// Sleep briefly before checking again
		time.Sleep(10 * time.Millisecond)

		// Check if connection is still alive
		if srv.IsConnectionClosed(clientConn) {
			return
		}
	}
}

func (h *XReadHandler) writeXreadResponse(clientConn net.Conn, results map[string][]database.StreamEntry, streamKeys []string) {
	// Count streams with results
	streamsWithData := 0
	for _, key := range streamKeys {
		if entries, exists := results[key]; exists && len(entries) > 0 {
			streamsWithData++
		}
	}

	if streamsWithData == 0 {
		clientConn.Write([]byte("$-1\r\n")) // null response
		return
	}

	// Build response: array of [stream_name, [entries...]]
	response := fmt.Sprintf("*%d\r\n", streamsWithData)

	for _, key := range streamKeys {
		if entries, exists := results[key]; exists && len(entries) > 0 {
			// Stream array: [stream_name, entries_array]
			response += "*2\r\n"

			// Stream name
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)

			// Entries array
			response += h.formatStreamEntries(entries)
		}
	}

	clientConn.Write([]byte(response))
}

func (h *XReadHandler) formatStreamEntries(entries []database.StreamEntry) string {
	// Start with array header
	response := fmt.Sprintf("*%d\r\n", len(entries))

	for _, entry := range entries {
		// Each entry is an array with 2 elements: [ID, [field1, value1, field2, value2, ...]]
		response += "*2\r\n"

		// 1. Entry ID as bulk string
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID)

		// 2. Fields array
		fieldCount := len(entry.Fields) * 2 // Each field has name and value
		response += fmt.Sprintf("*%d\r\n", fieldCount)

		// Add each field name and value as bulk strings
		for fieldName, fieldValue := range entry.Fields {
			// Field name
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(fieldName), fieldName)
			// Field value
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(fieldValue), fieldValue)
		}
	}

	return response
}
