package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"path"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/database"
)

func ReadArrayArguments(scanner *bufio.Scanner, conn net.Conn) ([]string, bool) {
	if !scanner.Scan() {
		fmt.Println("[ReadArrayArguments] Failed to scan array header")
		return nil, false
	}

	line := scanner.Text()

	if !strings.HasPrefix(line, "*") {
		fmt.Println("[ReadArrayArguments] Invalid array prefix, expected '*'")
		return nil, false
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, false
	}

	args := make([]string, count)
	for i := 0; i < count; i++ {
		// Read bulk string length
		if !scanner.Scan() {
			return nil, false
		}
		lengthLine := scanner.Text()

		if !strings.HasPrefix(lengthLine, "$") {
			return nil, false
		}

		length, err := strconv.Atoi(lengthLine[1:])
		if err != nil {
			return nil, false
		}

		// Read bulk string content
		if !scanner.Scan() {
			return nil, false
		}
		content := scanner.Text()

		// Handle cases where content might be shorter than expected
		if len(content) < length {
			remaining := length - len(content)
			buffer := make([]byte, remaining)
			n, err := conn.Read(buffer)
			if err != nil {
				return nil, false
			}
			content += string(buffer[:n])
		}

		args[i] = content
	}

	return args, true
}

func getKeysMatchingPattern(pattern string) []string {
	var results []string
	database.DB.Range(func(key, value interface{}) bool {
		strKey, ok := key.(string)
		if !ok {
			return true
		}

		match, err := path.Match(pattern, strKey)
		if err != nil || !match {
			return true
		}
		results = append(results, strKey)

		return true
	})
	return results
}

func WriteRaw(conn net.Conn, data []byte) {
	conn.Write(data)
}

func RandReplid() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		c := rand.Intn(len(chars))
		result[i] = chars[c]
	}
	return string(result)
}

func executeCommand(s *Server, clientConn net.Conn, cmd string, args []string) string {
	switch strings.ToUpper(cmd) {
	case "SET":
		return executeSetCommand(s, clientConn, args)
	case "GET":
		return executeGetCommand(s, clientConn, args)
	case "ECHO":
		return executeEchoCommand(s, clientConn, args)
	case "PING":
		return executePingCommand(s, clientConn, args)
	case "INCR":
		return executeIncrCommand(s, clientConn, args)
	default:
		return FormatError("ERR unknown command '" + cmd + "'")
	}
}

func executeSetCommand(s *Server, clientConn net.Conn, args []string) string {
	println("SET COMMADN IS CALLEd")
	if len(args) < 2 {
		return FormatError("ERR wrong number of arguments for 'SET'")
	}
	fmt.Printf("args: %+v\n", args)
	key, val := args[0], args[1]
	ms := -1
	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		ms, _ = strconv.Atoi(args[3])
	}

	database.SetKey(key, val, ms)

	if s.isMaster() {
		command := []string{"SET", key, val}
		if ms > -1 {
			command = append(command, "PX", strconv.Itoa(ms))
		}
		encoded := EncodeArray(command)
		s.replicationOffset += len(encoded)

		for i := 0; i < len(s.replicaConn); i++ {
			conn := s.replicaConn[i]
			bytesWritten, err := conn.Write([]byte(encoded))
			if err != nil {
				s.replicaConn = append(s.replicaConn[:i], s.replicaConn[i+1:]...)
				delete(s.replicaOffsets, conn)
				i--
				continue
			}
			s.replicaOffsets[conn] += bytesWritten
		}
	}

	return "+OK\r\n"
}

func executeGetCommand(s *Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return FormatError("ERR wrong number of arguments for 'GET'")
	}

	key := args[0]
	val, success := database.GetKey(key)
	if !success {
		return ""
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func executeIncrCommand(s *Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return FormatError("ERR wrong number of arguments for 'INCR'")
	}

	key := args[0]
	resp, success := database.Increment(key, 1)
	if !success {
		return FormatError("ERR value is not an integer or out of range")
	}

	receivedInt, err := strconv.Atoi(resp)
	if err != nil {
		return FormatError("ERR value is not an integer or out of range")
	}

	return fmt.Sprintf(":%d\r\n", receivedInt)
}

func executeEchoCommand(s *Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return FormatError("ERR wrong number of arguments for 'ECHO'")
	}

	return args[0]
}

func executePingCommand(s *Server, clientConn net.Conn, args []string) string {
	if len(args) == 0 {
		return FormatSimpleString("PONG")
	}
	return args[0]
}
