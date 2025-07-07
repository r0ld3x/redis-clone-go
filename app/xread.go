package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

func (c XReadCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	if len(commandArgs) < 3 {
		WriteError(clientConn, "ERR wrong number of arguments for 'XREAD'")
		return
	}

	var blockTimeout int64 = -1
	var argIndex = 0

	if strings.ToUpper(commandArgs[0]) == "BLOCK" {
		if len(commandArgs) < 5 {
			WriteError(clientConn, "ERR wrong number of arguments for 'XREAD'")
			return
		}

		var err error
		blockTimeout, err = strconv.ParseInt(commandArgs[1], 10, 64)
		if err != nil {
			WriteError(clientConn, "ERR timeout is not an integer or out of range")
			return
		}
		argIndex = 2
	}
	if strings.ToUpper(commandArgs[argIndex]) != "STREAMS" {
		WriteError(clientConn, "ERR syntax error")
		return
	}
	argIndex++

	// Parse streams and IDs
	remainingArgs := commandArgs[argIndex:]
	if len(remainingArgs)%2 != 0 {
		WriteError(clientConn, "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified")
		return
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
		c.performRead(s, clientConn, streamKeys, startIDs)
	} else {
		// Blocking read
		c.performBlockingRead(s, clientConn, streamKeys, startIDs, blockTimeout)
	}

}

func (c XReadCommandHandler) performRead(s *Server, clientConn net.Conn, streamKeys []string, startIDs []string) {
	results, err := database.StreamReadMultiple(streamKeys, startIDs)
	if err != nil {
		WriteError(clientConn, err.Error())
		return
	}

	// Format response
	c.writeXreadResponse(clientConn, results, streamKeys)
}

func (c XReadCommandHandler) performBlockingRead(s *Server, clientConn net.Conn, streamKeys []string, startIDs []string, blockTimeout int64) {
	startTime := time.Now()

	for {
		// Try to read
		results, err := database.StreamReadMultiple(streamKeys, startIDs)
		if err != nil {
			WriteError(clientConn, err.Error())
			return
		}

		// If we have results, return them
		if len(results) > 0 {
			c.writeXreadResponse(clientConn, results, streamKeys)
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
		if s.isConnectionClosed(clientConn) {
			return
		}
	}
}

func (c XReadCommandHandler) writeXreadResponse(clientConn net.Conn, results map[string][]database.StreamEntry, streamKeys []string) {
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
			response += formatStreamEntries(entries)
		}
	}

	clientConn.Write([]byte(response))
}
