package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

func (s *Server) isConnectionClosed(conn net.Conn) bool {
	// Try to read one byte with immediate timeout
	one := make([]byte, 1)
	conn.SetReadDeadline(time.Now())
	if _, err := conn.Read(one); err == io.EOF {
		logDebug("CONNECTION", "Connection closed detected for %s", conn.RemoteAddr())
		return true
	}
	// Reset deadline
	var zero time.Time
	conn.SetReadDeadline(zero)
	return false
}

func WriteInteger(conn net.Conn, value int) error {
	response := fmt.Sprintf(":%d\r\n", value)
	logDebug("WRITE", "Writing integer response: %s", strings.ReplaceAll(response, "\r\n", "\\r\\n"))

	n, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write integer %d: %v", value, err)
		return err
	}

	logDebug("WRITE", "Successfully wrote %d bytes for integer %d", n, value)

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			logError("WRITE", "Failed to set TCP_NODELAY: %v", err)
		}
	}

	return nil
}

// Add other missing write functions with logging
func WriteSimpleString(conn net.Conn, s string) {
	response := fmt.Sprintf("+%s\r\n", s)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write simple string '%s': %v", s, err)
	} else {
		logDebug("WRITE", "Wrote simple string: +%s", s)
	}
}

func WriteBulkString(conn net.Conn, s string) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write bulk string '%s': %v", s, err)
	} else {
		logDebug("WRITE", "Wrote bulk string (%d bytes): %s", len(s), s)
	}
}

func WriteError(conn net.Conn, errMsg string) {
	response := fmt.Sprintf("-%s\r\n", errMsg)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write error '%s': %v", errMsg, err)
	} else {
		logDebug("WRITE", "Wrote error: -%s", errMsg)
	}
}

func WriteArray(conn net.Conn, elements []string) {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	_, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write array %v: %v", elements, err)
	} else {
		logDebug("WRITE", "Wrote array (%d elements): %v", len(elements), elements)
	}
}

func WriteArray2(conn net.Conn, elements []string) {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += element
	}
	_, err := conn.Write([]byte(response))
	if err != nil {
		logError("WRITE", "Failed to write array %v: %v", elements, err)
	} else {
		logDebug("WRITE", "Wrote array (%d elements)", len(elements))
	}
}

func EncodeArray(elements []string) string {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	return response
}

func FormatInteger(value int) string {
	response := fmt.Sprintf(":%d\r\n", value)
	logDebug("FORMAT", "Formatted integer response: %s", strings.ReplaceAll(response, "\r\n", "\\r\\n"))
	return response
}

func FormatSimpleString(s string) string {
	response := fmt.Sprintf("%s\r\n", s)
	logDebug("FORMAT", "Formatted simple string: +%s", s)
	return response
}

func FormatBulkString(s string) string {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	logDebug("FORMAT", "Formatted bulk string (%d bytes): %s", len(s), s)
	return response
}

func FormatError(errMsg string) string {
	response := fmt.Sprintf("-%s\r\n", errMsg)
	logDebug("FORMAT", "Formatted error: -%s", errMsg)
	return response
}

func FormatArray(elements []string) string {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	logDebug("FORMAT", "Formatted array (%d elements): %v", len(elements), elements)
	return response
}

func formatStreamEntries(entries []database.StreamEntry) string {
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
