package protocol

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
)

var logger = logging.NewLogger("PROTOCOL")

// ReadArrayArguments reads RESP array arguments from a connection
func ReadArrayArguments(reader *bufio.Reader) ([]string, bool) {
	// Read array header: *<count>\r\n
	line, err := reader.ReadString('\n')
	if err != nil {
		logger.Debug("failed to read array header: %v", err)
		return nil, false
	}
	line = strings.TrimSpace(line)

	if !strings.HasPrefix(line, "*") {
		logger.Debug("Invalid array prefix, expected '*', got: %s", line)
		return nil, false
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		logger.Debug("invalid array length: %v", err)
		return nil, false
	}

	args := make([]string, count)

	for i := 0; i < count; i++ {
		// Read bulk string header: $<len>\r\n
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			logger.Debug("failed to read bulk string length: %v", err)
			return nil, false
		}
		lengthLine = strings.TrimSpace(lengthLine)

		if !strings.HasPrefix(lengthLine, "$") {
			logger.Debug("Invalid bulk string prefix, expected '$', got: %s", lengthLine)
			return nil, false
		}

		length, err := strconv.Atoi(lengthLine[1:])
		if err != nil {
			logger.Debug("invalid bulk string length: %v", err)
			return nil, false
		}

		if length < 0 {
			args[i] = "" // RESP null bulk string
			continue
		}

		// Read <length> bytes of content
		buf := make([]byte, length)
		if _, err := io.ReadFull(reader, buf); err != nil {
			logger.Debug("failed to read bulk string content: %v", err)
			return nil, false
		}

		args[i] = string(buf)

		// Read trailing \r\n
		if _, err := reader.Discard(2); err != nil {
			logger.Debug("failed to discard CRLF: %v", err)
			return nil, false
		}
	}

	return args, true
}

// WriteInteger writes a RESP integer response
func WriteInteger(conn net.Conn, value int) error {
	response := fmt.Sprintf(":%d\r\n", value)
	logger.Debug("Writing integer response: %s", strings.ReplaceAll(response, "\r\n", "\\r\\n"))

	n, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write integer %d: %v", value, err)
		return err
	}

	logger.Debug("Successfully wrote %d bytes for integer %d", n, value)

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			logger.Error("Failed to set TCP_NODELAY: %v", err)
		}
	}

	return nil
}

// WriteSimpleString writes a RESP simple string response
func WriteSimpleString(conn net.Conn, s string) {
	response := fmt.Sprintf("+%s\r\n", s)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write simple string '%s': %v", s, err)
	} else {
		logger.Debug("Wrote simple string: +%s", s)
	}
}

// WriteBulkString writes a RESP bulk string response
func WriteBulkString(conn net.Conn, s string) {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write bulk string '%s': %v", s, err)
	} else {
		logger.Debug("Wrote bulk string (%d bytes): %s", len(s), s)
	}
}

// WriteError writes a RESP error response
func WriteError(conn net.Conn, errMsg string) {
	response := fmt.Sprintf("-%s\r\n", errMsg)
	_, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write error '%s': %v", errMsg, err)
	} else {
		logger.Debug("Wrote error: -%s", errMsg)
	}
}

// WriteArray writes a RESP array response
func WriteArray(conn net.Conn, elements []string) {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	_, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write array %v: %v", elements, err)
	} else {
		logger.Debug("Wrote array (%d elements): %v", len(elements), elements)
	}
}

// WriteArray2 writes a RESP array response with pre-formatted elements
func WriteArray2(conn net.Conn, elements []string) {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += element
	}
	_, err := conn.Write([]byte(response))
	if err != nil {
		logger.Error("Failed to write array %v: %v", elements, err)
	} else {
		logger.Debug("Wrote array (%d elements)", len(elements))
	}
}

// EncodeArray encodes an array of strings into RESP format
func EncodeArray(elements []string) string {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	return response
}

// Format functions for building responses
func FormatInteger(value int) string {
	response := fmt.Sprintf(":%d\r\n", value)
	logger.Debug("Formatted integer response: %s", strings.ReplaceAll(response, "\r\n", "\\r\\n"))
	return response
}

func FormatSimpleString(s string) string {
	response := fmt.Sprintf("+%s\r\n", s)
	logger.Debug("Formatted simple string: +%s", s)
	return response
}

func FormatBulkString(s string) string {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	logger.Debug("Formatted bulk string (%d bytes): %s", len(s), s)
	return response
}

func FormatError(errMsg string) string {
	response := fmt.Sprintf("-%s\r\n", errMsg)
	logger.Debug("Formatted error: -%s", errMsg)
	return response
}

func FormatArray(elements []string) string {
	response := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(element), element)
	}
	logger.Debug("Formatted array (%d elements): %v", len(elements), elements)
	return response
}

// WriteRaw writes raw data to connection
func WriteRaw(conn net.Conn, data []byte) {
	conn.Write(data)
}
