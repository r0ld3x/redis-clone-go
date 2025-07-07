package main

import (
	"bufio"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/database"
	"github.com/codecrafters-io/redis-starter-go/app/rdb"
)

var _ = net.Listen
var _ = os.Exit

func logInfo(component, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s]", timestamp, component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func logError(component, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] ❌ ERROR:", timestamp, component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func logSuccess(component, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] ✅ SUCCESS:", timestamp, component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func logDebug(component, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	prefix := fmt.Sprintf("[%s] [%s] 🔍 DEBUG:", timestamp, component)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

func logNetwork(component, direction, message string, args ...interface{}) {
	timestamp := time.Now().Format("15:04:05.000")
	arrow := "📤 OUT"
	if direction == "IN" {
		arrow = "📥 IN"
	}
	prefix := fmt.Sprintf("[%s] [%s] %s:", timestamp, component, arrow)
	fmt.Printf(prefix+" "+message+"\n", args...)
}

type Transaction struct {
	commands      []QueuedCommand
	inTransaction bool
}

type QueuedCommand struct {
	command string
	args    []string
}

type Config struct {
	Directory     string
	DBFileName    string
	HostName      string
	Port          string
	Role          string
	MasterAddress string
}

type Server struct {
	config            Config
	replicaConn       []net.Conn
	masterConn        net.Conn
	replicationOffset int
	replicationID     string
	replicaOffsets    map[net.Conn]int
	ackReceived       chan net.Conn
	handshakeComplete bool
	transactions      map[net.Conn]*Transaction
}

type CommandHandler interface {
	handle(s *Server, clientConn net.Conn, commandArgs []string)
}

type Command string

const (
	CommandCommand  Command = "COMMAND"
	EchoCommand     Command = "ECHO"
	PingCommand     Command = "PING"
	GetCommand      Command = "GET"
	SetCommand      Command = "SET"
	ConfigCommand   Command = "CONFIG"
	KeysCommand     Command = "KEYS"
	InfoCommand     Command = "INFO"
	ReplconfCommand Command = "REPLCONF"
	PsyncCommand    Command = "PSYNC"
	WaitCommand     Command = "WAIT"
	IncrCommand     Command = "INCR"
	MultiCommand    Command = "MULTI"
	ExecCommand     Command = "EXEC"
	DiscardCommand  Command = "DISCARD"
	TypeCommand     Command = "TYPE"
	XAddCommand     Command = "XADD"
	XRangeCommand   Command = "XRANGE"
	XReadCommand    Command = "XREAD"
)

var WriteCommands = []Command{SetCommand}

type EchoCommandHandler struct{}
type PingCommandHandler struct{}
type GetCommandHandler struct{}
type SetCommandHandler struct{}
type ConfigCommandHandler struct{}
type KeysCommandHandler struct{}
type InfoCommandHandler struct{}
type ReplconfCommandHandler struct{}
type PsyncCommandHandler struct{}
type WaitCommandHandler struct{}
type CommandCommandHandler struct{}
type IncrCommandHandler struct{}
type MultiCommandHandler struct{}
type ExecCommandHandler struct{}
type DiscardCommandHandler struct{}
type TypeCommandHandler struct{}
type XAddCommandHandler struct{}
type XRangeCommandHandler struct{}
type XReadCommandHandler struct{}

func (c EchoCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("ECHO", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 1 {
		logError("ECHO", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'ECHO'")
		return
	}

	logNetwork("ECHO", "OUT", "Sending bulk string response: %s", commandArgs[0])
	WriteBulkString(clientConn, commandArgs[0])
	logSuccess("ECHO", "Command completed successfully")
}

func (c CommandCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("COMMAND", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)
	logNetwork("COMMAND", "OUT", "Sending OK response")
	WriteSimpleString(clientConn, "OK")
	logSuccess("COMMAND", "Command completed successfully")
}

func (c PingCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("PING", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)
	logNetwork("PING", "OUT", "Sending PONG response")
	WriteSimpleString(clientConn, "PONG")
	logSuccess("PING", "Command completed successfully")
}

func (c GetCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("GET", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 1 {
		logError("GET", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'GET'")
		return
	}

	key := commandArgs[0]
	logDebug("GET", "Looking up key: %s", key)

	val, success := database.GetKey(key)
	if !success {
		logInfo("GET", "Key not found: %s", key)
		logNetwork("GET", "OUT", "Sending null response")
		clientConn.Write([]byte("$-1\r\n"))
		return
	}

	logInfo("GET", "Key found: %s = %s", key, val)
	logNetwork("GET", "OUT", "Sending value: %s", val)
	WriteSimpleString(clientConn, val)
	logSuccess("GET", "Command completed successfully")
}

func (c SetCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("SET", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 2 {
		logError("SET", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'SET'")
		return
	}

	if !s.isMaster() {
		logError("SET", "Attempted write on replica from %s", clientConn.RemoteAddr())
		WriteError(clientConn, "READONLY You can't write against a read only replica.")
		return
	}

	key, val := commandArgs[0], commandArgs[1]
	ms := -1
	if len(commandArgs) == 4 && strings.ToUpper(commandArgs[2]) == "PX" {
		ms, _ = strconv.Atoi(commandArgs[3])
	}

	logDebug("SET", "Storing key=%s value=%s TTL(ms)=%d", key, val, ms)
	database.SetKey(key, val, ms)
	logInfo("SET", "Key stored successfully: %s = %s", key, val)

	// Build the full SET command for replication
	command := []string{"SET", key, val}
	if ms > -1 {
		command = append(command, "PX", strconv.Itoa(ms))
	}
	encoded := EncodeArray(command)

	// Update master's replication offset
	oldOffset := s.replicationOffset
	s.replicationOffset += len(encoded)
	logDebug("SET", "Updated master replication offset: %d -> %d (+%d bytes)",
		oldOffset, s.replicationOffset, len(encoded))

	logInfo("SET", "Replicating to %d replicas", len(s.replicaConn))
	for i := 0; i < len(s.replicaConn); i++ {
		conn := s.replicaConn[i]
		logNetwork("SET", "OUT", "Sending SET command to replica %s", conn.RemoteAddr())

		bytesWritten, err := conn.Write([]byte(encoded))
		if err != nil {
			logError("SET", "Replica %s disconnected: %v", conn.RemoteAddr(), err)
			// Remove disconnected replica
			s.replicaConn = append(s.replicaConn[:i], s.replicaConn[i+1:]...)
			delete(s.replicaOffsets, conn)
			i--
			continue
		}

		oldReplicaOffset := s.replicaOffsets[conn]
		s.replicaOffsets[conn] += bytesWritten
		logDebug("SET", "Updated replica %s offset: %d -> %d (+%d bytes)",
			conn.RemoteAddr(), oldReplicaOffset, s.replicaOffsets[conn], bytesWritten)
	}

	// Respond to client
	logNetwork("SET", "OUT", "Sending OK response to client")
	WriteSimpleString(clientConn, "OK")
	logSuccess("SET", "Command completed successfully")
}

func (c ConfigCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("CONFIG", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 2 {
		logError("CONFIG", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'CONFIG'")
		return
	}

	cmd, name := strings.ToUpper(commandArgs[0]), strings.ToUpper(commandArgs[1])
	logDebug("CONFIG", "Processing subcommand: %s %s", cmd, name)

	if cmd == "GET" {
		switch name {
		case "DIR":
			logInfo("CONFIG", "Returning directory: %s", s.config.Directory)
			WriteArray(clientConn, []string{"dir", s.config.Directory})
		case "DBFILENAME":
			logInfo("CONFIG", "Returning DB filename: %s", s.config.DBFileName)
			WriteArray(clientConn, []string{"dbfilename", s.config.DBFileName})
		default:
			logError("CONFIG", "Unsupported parameter: %s", name)
			WriteError(clientConn, "unsupported CONFIG parameter")
		}
	}
	logSuccess("CONFIG", "Command completed successfully")
}

func (c KeysCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("KEYS", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 1 {
		logError("KEYS", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'KEYS'")
		return
	}

	pattern := commandArgs[0]
	logDebug("KEYS", "Searching for pattern: %s", pattern)

	data := getKeysMatchingPattern(pattern)
	logInfo("KEYS", "Found %d keys matching pattern %s", len(data), pattern)

	logNetwork("KEYS", "OUT", "Sending array response with %d keys", len(data))
	WriteArray(clientConn, data)
	logSuccess("KEYS", "Command completed successfully")
}

func (c InfoCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("INFO", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	info := "# Replication\n"
	info += fmt.Sprintf("role:%s\r\n", s.config.Role)

	if s.config.Role == "slave" {
		info += fmt.Sprintf("master_host:%s\r\n", s.config.HostName)
		info += fmt.Sprintf("master_port:%s\r\n", s.config.Port)
	}
	info += fmt.Sprintf("master_replid:%s\r\n", s.replicationID)
	info += fmt.Sprintf("master_repl_offset:%d\r\n", s.replicationOffset)

	logDebug("INFO", "Generated info response: %s", strings.ReplaceAll(info, "\r\n", "\\r\\n"))
	logNetwork("INFO", "OUT", "Sending bulk string response")
	WriteBulkString(clientConn, info)
	logSuccess("INFO", "Command completed successfully")
}

func (c ReplconfCommandHandler) handle(s *Server, clientConn net.Conn, args []string) {
	logInfo("REPLCONF", "==================== REPLCONF COMMAND START ====================")
	logInfo("REPLCONF", "Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		logError("REPLCONF", "Wrong number of arguments: %d", len(args))
		WriteError(clientConn, "wrong number of arguments for 'REPLCONF'")
		return
	}

	subcommand := strings.ToUpper(args[0])
	logDebug("REPLCONF", "Processing subcommand: %s", subcommand)

	switch subcommand {
	case "LISTENING-PORT":
		logInfo("REPLCONF", "Handling LISTENING-PORT from %s", clientConn.RemoteAddr())
		logNetwork("REPLCONF", "OUT", "Sending OK response for LISTENING-PORT")
		WriteSimpleString(clientConn, "OK")
		logSuccess("REPLCONF", "LISTENING-PORT handled successfully")

	case "GETACK":
		logInfo("REPLCONF", "Handling GETACK request from %s", clientConn.RemoteAddr())

		// For replicas, send their local offset
		if s.config.Role == "slave" {
			logDebug("REPLCONF", "Replica responding with local offset: %d", s.replicationOffset)
			logNetwork("REPLCONF", "OUT", "Sending ACK with offset %d", s.replicationOffset)
			WriteArray(clientConn, []string{"REPLCONF", "ACK", strconv.Itoa(s.replicationOffset)})
			logSuccess("REPLCONF", "Sent ACK with replica offset %d", s.replicationOffset)
		} else {
			// For master handling replica's GETACK, send replica's offset
			offset, exists := s.replicaOffsets[clientConn]
			if !exists {
				offset = 0
				logDebug("REPLCONF", "Replica offset not found, using 0")
			}
			logDebug("REPLCONF", "Master responding with replica offset: %d", offset)
			logNetwork("REPLCONF", "OUT", "Sending ACK with offset %d", offset)
			WriteArray(clientConn, []string{"REPLCONF", "ACK", strconv.Itoa(offset)})
			logSuccess("REPLCONF", "Sent ACK with offset %d", offset)
		}

	case "ACK":
		if len(args) >= 2 {
			offset, err := strconv.Atoi(args[1])
			if err == nil {
				s.replicaOffsets[clientConn] = offset
				logDebug("REPLCONF", "Updated replica offset: %s -> %d", clientConn.RemoteAddr(), offset)

				select {
				case s.ackReceived <- clientConn:
					logDebug("REPLCONF", "Successfully signaled ACK to WAIT command")
				default:
					logDebug("REPLCONF", "ACK channel full")
				}
			}
		}

	case "CAPA":
		logInfo("REPLCONF", "Handling CAPA from %s", clientConn.RemoteAddr())
		logNetwork("REPLCONF", "OUT", "Sending OK response for CAPA")
		WriteSimpleString(clientConn, "OK")
		logSuccess("REPLCONF", "CAPA handled successfully")

	default:
		logInfo("REPLCONF", "Handling unknown subcommand '%s' from %s", subcommand, clientConn.RemoteAddr())
		logNetwork("REPLCONF", "OUT", "Sending OK response for unknown subcommand")
		WriteSimpleString(clientConn, "OK")
		logSuccess("REPLCONF", "Unknown subcommand handled with OK")
	}

	logInfo("REPLCONF", "==================== REPLCONF COMMAND END ====================")
}

func (c PsyncCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("PSYNC", "==================== PSYNC COMMAND START ====================")
	logInfo("PSYNC", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) != 2 {
		logError("PSYNC", "Invalid argument count: %d", len(commandArgs))
		WriteError(clientConn, "ERR invalid PSYNC arguments")
		return
	}

	replID := commandArgs[0]
	offset := commandArgs[1]
	logDebug("PSYNC", "Replication ID: %s, Offset: %s", replID, offset)

	if replID == "?" && offset == "-1" {
		logInfo("PSYNC", "Performing FULLRESYNC for %s", clientConn.RemoteAddr())

		// Add to replica connections
		s.replicaConn = append(s.replicaConn, clientConn)
		s.replicaOffsets[clientConn] = 0
		logDebug("PSYNC", "Added replica to connections list. Total replicas: %d", len(s.replicaConn))

		fullresyncResp := fmt.Sprintf("FULLRESYNC %s %d", s.replicationID, s.replicationOffset)
		logNetwork("PSYNC", "OUT", "Sending FULLRESYNC response: %s", fullresyncResp)
		WriteSimpleString(clientConn, fullresyncResp)

		// Send empty RDB file
		rdb := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
		dst := make([]byte, base64.StdEncoding.DecodedLen(len(rdb)))
		n, err := base64.StdEncoding.Decode(dst, []byte(rdb))
		if err != nil {
			logError("PSYNC", "Failed to decode base64 RDB: %v", err)
			return
		}
		dst = dst[:n]

		logNetwork("PSYNC", "OUT", "Sending RDB file (%d bytes)", len(dst))
		clientConn.Write([]byte(fmt.Sprintf("$%v\r\n", len(dst))))
		clientConn.Write(dst)
		logSuccess("PSYNC", "FULLRESYNC completed for %s", clientConn.RemoteAddr())
	} else {
		// Partial resync
		logInfo("PSYNC", "Attempting partial resync with replID=%s offset=%s", replID, offset)
		logNetwork("PSYNC", "OUT", "Sending CONTINUE response")
		WriteSimpleString(clientConn, "CONTINUE")

		// Ensure replica is in the list if not already
		found := false
		for _, rconn := range s.replicaConn {
			if rconn == clientConn {
				found = true
				break
			}
		}
		if !found {
			s.replicaConn = append(s.replicaConn, clientConn)
			s.replicaOffsets[clientConn] = 0
			logDebug("PSYNC", "Added replica to connections list for partial resync")
		}
		logSuccess("PSYNC", "Partial resync setup completed")
	}

	logInfo("PSYNC", "==================== PSYNC COMMAND END ====================")
}

func (c WaitCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("WAIT", "==================== WAIT COMMAND START ====================")
	logInfo("WAIT", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 2 {
		logError("WAIT", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'WAIT'")
		return
	}

	count, err1 := strconv.Atoi(commandArgs[0])
	timeout, err2 := strconv.Atoi(commandArgs[1])

	if err1 != nil || err2 != nil {
		logError("WAIT", "Failed to parse arguments: count_err=%v, timeout_err=%v", err1, err2)
		WriteError(clientConn, "invalid arguments for 'WAIT'")
		return
	}

	logInfo("WAIT", "Need %d acks within %d ms", count, timeout)
	logInfo("WAIT", "Connected replicas: %d", len(s.replicaConn))

	if len(s.replicaConn) == 0 {
		logInfo("WAIT", "No replicas, returning 0")
		WriteInteger(clientConn, 0)
		return
	}

	// Send GETACK to all replicas
	for _, conn := range s.replicaConn {
		go func(conn net.Conn) {
			conn.Write([]byte(EncodeArray([]string{"REPLCONF", "GETACK", "*"})))
		}(conn)
	}

	acks := 0
	for _, conn := range s.replicaConn {
		fmt.Println("s.replicaOffsets[conn] ", s.replicaOffsets[conn])
		if s.replicaOffsets[conn] <= 0 {
			acks++
		}
	}
	timer := time.After(time.Duration(timeout) * time.Millisecond)

outer:
	for acks < count {
		select {
		case <-s.ackReceived:
			acks++
			logInfo("WAIT", "ACK received, total: %d", acks)
		case <-timer:
			logInfo("WAIT", "Timeout reached, total: %d", acks)
			break outer
		}
	}

	logInfo("WAIT", "Returning %d acks", acks)
	WriteInteger(clientConn, acks)
	logInfo("WAIT", "==================== WAIT COMMAND END ====================")
}

func (c IncrCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("INCR", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)

	if len(commandArgs) < 1 {
		logError("INCR", "Wrong number of arguments: %d", len(commandArgs))
		WriteError(clientConn, "wrong number of arguments for 'INCR'")
		return
	}
	key := commandArgs[0]
	var by = 1
	if len(commandArgs) > 1 {
		Int, err := strconv.Atoi(commandArgs[1])
		if err == nil {
			by = Int
		}
	}
	resp, success := database.Increment(key, by)
	if !success {
		WriteError(clientConn, "ERR value is not an integer or out of range")
		return
	}

	receivedInt, err := strconv.Atoi(resp)
	if err != nil {
		WriteError(clientConn, "failed to convert response to integer")
		return
	}

	WriteInteger(clientConn, receivedInt)
	logSuccess("INCR", "Command completed successfully")
}

func (c MultiCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	logInfo("MULTI", "Command received from %s with args: %v", clientConn.RemoteAddr(), commandArgs)
	if s.transactions[clientConn] != nil && s.transactions[clientConn].inTransaction {
		WriteError(clientConn, "MULTI calls can not be nested")
		return
	}

	s.transactions[clientConn] = &Transaction{
		commands:      make([]QueuedCommand, 0),
		inTransaction: true,
	}
	WriteSimpleString(clientConn, "OK")
	logSuccess("MULTI", "Command completed successfully")
}

func (c ExecCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	transaction := s.transactions[clientConn]
	if transaction == nil || !transaction.inTransaction {
		WriteError(clientConn, "ERR EXEC without MULTI")
		return
	}

	results := make([]string, 0)

	for _, queuedCmd := range transaction.commands {
		fmt.Printf("queuedCmd: %+v\n", queuedCmd)
		result := executeCommand(s, clientConn, queuedCmd.command, queuedCmd.args)
		log.Printf("%+v\n", result)
		results = append(results, result)
	}

	WriteArray2(clientConn, results)
	delete(s.transactions, clientConn)
}

func (c TypeCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	if len(commandArgs) > 1 {
		WriteError(clientConn, "ERR no key provided")
		return
	}
	key := commandArgs[0]
	response, found := database.GetType(key)
	log.Printf("response: %+v\n", response)
	if !found {
		WriteSimpleString(clientConn, "none")
		return
	}
	WriteSimpleString(clientConn, response)

}

func (c XAddCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	if len(commandArgs) < 3 {
		WriteError(clientConn, "ERR wrong number of arguments for 'XADD'")
		return
	}

	key := commandArgs[0]
	id := commandArgs[1]
	if id == "0-0" {
		WriteError(clientConn, "ERR The ID specified in XADD must be greater than 0-0")
		return
	}
	fields := commandArgs[2:]

	entryID, err := database.StreamAdd(key, id, fields)
	if err != nil {
		WriteError(clientConn, err.Error())
		return
	}

	WriteBulkString(clientConn, entryID)

}
func (c XRangeCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	if len(commandArgs) < 3 {
		WriteError(clientConn, "ERR wrong number of arguments for 'XADD'")
		return
	}

	key := commandArgs[0]
	start := commandArgs[1]
	end := commandArgs[2]

	entries, err := database.StreamRange(key, start, end)
	if err != nil {
		WriteError(clientConn, err.Error())
		return
	}

	respData := formatStreamEntries(entries)
	clientConn.Write([]byte(respData))
	fmt.Printf("respData: %q\n", respData)
	// WriteArray(clientConn, entryID)

}

func getCommandHandler(command Command) CommandHandler {
	logDebug("HANDLER", "Getting handler for command: %s", command)

	switch command {
	case PingCommand:
		return PingCommandHandler{}
	case EchoCommand:
		return EchoCommandHandler{}
	case GetCommand:
		return GetCommandHandler{}
	case SetCommand:
		return SetCommandHandler{}
	case KeysCommand:
		return KeysCommandHandler{}
	case ConfigCommand:
		return ConfigCommandHandler{}
	case InfoCommand:
		return InfoCommandHandler{}
	case ReplconfCommand:
		return ReplconfCommandHandler{}
	case PsyncCommand:
		return PsyncCommandHandler{}
	case WaitCommand:
		return WaitCommandHandler{}
	case CommandCommand:
		return CommandCommandHandler{}
	case IncrCommand:
		return IncrCommandHandler{}
	case MultiCommand:
		return MultiCommandHandler{}
	case ExecCommand:
		return ExecCommandHandler{}
	case DiscardCommand:
		return DiscardCommandHandler{}
	case TypeCommand:
		return TypeCommandHandler{}
	case XAddCommand:
		return XAddCommandHandler{}
	case XRangeCommand:
		return XRangeCommandHandler{}
	case XReadCommand:
		return XReadCommandHandler{}
	default:
		logError("HANDLER", "No command handler found for command: %s", command)
		return nil
	}
}

func (s *Server) isMaster() bool {
	return s.config.Role == "master"
}

func main() {
	logInfo("MAIN", "Starting Redis server...")
	database.Start()

	dir := flag.String("dir", "", "Directory to store the database")
	dbfilename := flag.String("dbfilename", "", "Database file name")
	port := flag.Int("port", 6379, "Port to run the server on")
	replicaof := flag.String("replicaof", "", "Master address if this is a replica (format: host port)")

	flag.Parse()

	config := Config{
		Directory:  *dir,
		DBFileName: *dbfilename,
		HostName:   "localhost",
		Port:       fmt.Sprintf("%d", *port),
		Role:       "master",
	}

	logInfo("MAIN", "Server configuration: %+v", config)

	server := &Server{
		config:            config,
		replicaOffsets:    make(map[net.Conn]int),
		replicationID:     RandReplid(),
		replicationOffset: 0,
		ackReceived:       make(chan net.Conn, 100),
		transactions:      make(map[net.Conn]*Transaction, 100),
	}

	if *replicaof != "" {
		parts := strings.Fields(*replicaof)
		if len(parts) != 2 {
			log.Fatalf("Invalid --replicaof format, expected: <host> <port>")
		}
		server.config.Role = "slave"
		server.config.MasterAddress = fmt.Sprintf("%s:%s", parts[0], parts[1])

		logInfo("MAIN", "Connecting to master at %s", server.config.MasterAddress)
		var err error
		server.masterConn, err = net.Dial("tcp", server.config.MasterAddress)
		if err != nil {
			log.Fatalln("couldn't connect to master at", server.config.MasterAddress)
		}
		logSuccess("MAIN", "Connected to master successfully")
		go server.sendHandshake()
	}

	if server.config.Role == "master" && server.config.DBFileName != "" {
		logInfo("MAIN", "Loading RDB file: %s", server.config.Directory+"/"+server.config.DBFileName)
		rdb.ParseRDB(server.config.Directory + "/" + server.config.DBFileName)
	}

	listenAddress := fmt.Sprintf("%s:%s", server.config.HostName, server.config.Port)
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddress, err)
	}
	defer l.Close()

	logSuccess("MAIN", "[%s] Server listening on %s", server.config.Role, listenAddress)

	for {
		conn, err := l.Accept()
		if err != nil {
			logError("MAIN", "Accept error: %v", err)
			continue
		}
		logInfo("CONNECTION", "New connection established from: %s", conn.RemoteAddr())
		go server.handleNewConnection(conn)
	}
}

func (s *Server) handleNewConnection(conn net.Conn) {
	logInfo("CONNECTION", "Starting connection handler for %s", conn.RemoteAddr())
	defer conn.Close()
	defer s.removeReplica(conn)
	scanner := bufio.NewScanner(conn)

	for {
		if s.isConnectionClosed(conn) {
			logInfo("CONNECTION", "Connection closed by client: %s", conn.RemoteAddr())
			return
		}

		logDebug("CONNECTION", "Waiting for command from %s", conn.RemoteAddr())
		args, ok := ReadArrayArguments(scanner, conn)
		if !ok {
			logInfo("CONNECTION", "Connection closed or error reading from: %s", conn.RemoteAddr())
			return
		}

		logNetwork("CONNECTION", "IN", "Received command from %s: %v", conn.RemoteAddr(), args)

		if len(args) < 1 {
			logError("CONNECTION", "Empty command received from %s", conn.RemoteAddr())
			WriteError(conn, "ERR parsing args")
			continue
		}

		logNetwork("CONNECTION", "IN", "Received command from %s: %v", conn.RemoteAddr(), args)

		cmd := strings.ToUpper(args[0])
		commandArgs := args[1:]
		fmt.Printf("cmd: %+v\n", cmd)
		fmt.Printf("commandArgs: %+v\n", commandArgs)
		transaction := s.transactions[conn]

		if transaction != nil && transaction.inTransaction {
			if cmd == "EXEC" || cmd == "DISCARD" || cmd == "MULTI" {
				handler := getCommandHandler(Command(cmd))
				handler.handle(s, conn, commandArgs)
			} else {
				queue := QueuedCommand{
					command: cmd,
					args:    commandArgs,
				}
				transaction.commands = append(transaction.commands, queue)
				WriteSimpleString(conn, "QUEUED")
			}
		} else {

			logDebug("CONNECTION", "Processing command: '%s' with args: %v", cmd, commandArgs)

			handler := getCommandHandler(Command(cmd))
			if handler != nil {
				logDebug("CONNECTION", "Found handler for command: %s", cmd)
				handler.handle(s, conn, commandArgs)
			} else {
				logError("CONNECTION", "No handler found for command: %s", cmd)
				WriteError(conn, "unknown command '"+cmd+"'")
			}
		}

	}
}

func (c DiscardCommandHandler) handle(s *Server, clientConn net.Conn, commandArgs []string) {
	transaction := s.transactions[clientConn]

	if transaction == nil || !transaction.inTransaction {
		WriteError(clientConn, "ERR DISCARD without MULTI")
		return
	}

	delete(s.transactions, clientConn)
	WriteSimpleString(clientConn, "OK")
}

func (s *Server) sendHandshake() {
	logInfo("HANDSHAKE", "==================== HANDSHAKE START ====================")
	reader := bufio.NewReader(s.masterConn)

	logInfo("HANDSHAKE", "Starting handshake with master %s", s.config.MasterAddress)

	// Step 1: PING
	logNetwork("HANDSHAKE", "OUT", "Sending PING to master")
	WriteArray(s.masterConn, []string{"PING"})
	expectSimpleString(reader, "PONG")
	logSuccess("HANDSHAKE", "PING handshake successful")

	// Step 2: REPLCONF listening-port
	logNetwork("HANDSHAKE", "OUT", "Sending REPLCONF listening-port %s", s.config.Port)
	WriteArray(s.masterConn, []string{"REPLCONF", "listening-port", s.config.Port})
	expectSimpleString(reader, "OK")
	logSuccess("HANDSHAKE", "REPLCONF listening-port handshake successful")

	// Step 3: REPLCONF capa
	logNetwork("HANDSHAKE", "OUT", "Sending REPLCONF capa psync2")
	WriteArray(s.masterConn, []string{"REPLCONF", "capa", "psync2"})
	expectSimpleString(reader, "OK")
	logSuccess("HANDSHAKE", "REPLCONF capa handshake successful")

	// Step 4: PSYNC
	logNetwork("HANDSHAKE", "OUT", "Sending PSYNC ? -1")
	WriteArray(s.masterConn, []string{"PSYNC", "?", "-1"})
	line, _ := reader.ReadString('\n')
	logNetwork("HANDSHAKE", "IN", "PSYNC response: %s", strings.TrimSpace(line))

	if strings.HasPrefix(line, "+FULLRESYNC") {
		parts := strings.Split(strings.TrimSpace(line), " ")
		if len(parts) >= 3 {
			s.replicationID = parts[1]
			logDebug("HANDSHAKE", "Set replication ID: %s", s.replicationID)
		}
	}

	// Read RDB file
	rdbHeader, _ := reader.ReadString('\n') // $<rdbLen>
	rdbLenStr := strings.TrimSpace(rdbHeader[1:])
	rdbLen, _ := strconv.Atoi(rdbLenStr)
	logInfo("HANDSHAKE", "Reading RDB file of %d bytes", rdbLen)

	// Skip RDB content
	io.CopyN(io.Discard, reader, int64(rdbLen))
	logDebug("HANDSHAKE", "RDB file content skipped")

	s.handshakeComplete = true
	logSuccess("HANDSHAKE", "PSYNC handshake successful")

	// Reset replication offset after handshake
	s.replicationOffset = 0
	logInfo("HANDSHAKE", "Reset replication offset to 0 after handshake")
	logInfo("HANDSHAKE", "==================== HANDSHAKE END ====================")

	// Create a new scanner from the reader after handshake
	scanner := bufio.NewScanner(reader)

	logInfo("REPLICA", "Starting to handle commands from master")
	for {
		if s.isConnectionClosed(s.masterConn) {
			logError("REPLICA", "Connection to master lost")
			return
		}

		args, ok := ReadArrayArguments(scanner, s.masterConn)
		if !ok {
			logError("REPLICA", "Connection to master lost or error reading")
			return
		}

		logNetwork("REPLICA", "IN", "Received command from master: %v", args)

		if len(args) == 0 {
			continue
		}

		commandBytes := len(EncodeArray(args))
		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			// Update offset for PING
			oldOffset := s.replicationOffset
			s.replicationOffset += commandBytes
			logDebug("REPLICA", "Updated replication offset for PING: %d -> %d (+%d bytes)",
				oldOffset, s.replicationOffset, commandBytes)
			logInfo("REPLICA", "Received PING from master, offset now: %d", s.replicationOffset)

		case "SET":
			// Update offset for SET
			oldOffset := s.replicationOffset
			s.replicationOffset += commandBytes
			logDebug("REPLICA", "Updated replication offset for SET: %d -> %d (+%d bytes)",
				oldOffset, s.replicationOffset, commandBytes)

			if len(args) >= 3 {
				key := args[1]
				val := args[2]
				ms := -1
				if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
					ms, _ = strconv.Atoi(args[4])
				}
				database.SetKey(key, val, ms)
				logInfo("REPLICA", "Applied SET %s=%s (TTL: %d ms), offset now: %d", key, val, ms, s.replicationOffset)
			}

		case "REPLCONF":
			if len(args) >= 2 {
				subcommand := strings.ToUpper(args[1])
				switch subcommand {
				case "GETACK":
					// CRITICAL: Respond with current offset BEFORE updating it
					logInfo("REPLICA", "Received GETACK, responding with ACK %d", s.replicationOffset)
					logNetwork("REPLICA", "OUT", "Sending ACK with offset %d", s.replicationOffset)
					WriteArray(s.masterConn, []string{"REPLCONF", "ACK", strconv.Itoa(s.replicationOffset)})

					// Update offset AFTER responding
					oldOffset := s.replicationOffset
					s.replicationOffset += commandBytes
					logDebug("REPLICA", "Updated replication offset for GETACK: %d -> %d (+%d bytes)",
						oldOffset, s.replicationOffset, commandBytes)
				default:
					// Update offset for other REPLCONF commands
					oldOffset := s.replicationOffset
					s.replicationOffset += commandBytes
					logDebug("REPLICA", "Updated replication offset for REPLCONF %s: %d -> %d (+%d bytes)",
						subcommand, oldOffset, s.replicationOffset, commandBytes)
					logInfo("REPLICA", "Received REPLCONF %s, offset now: %d", subcommand, s.replicationOffset)
				}
			}

		default:
			// Update offset for any other commands
			oldOffset := s.replicationOffset
			s.replicationOffset += commandBytes
			logDebug("REPLICA", "Updated replication offset for %s: %d -> %d (+%d bytes)",
				cmd, oldOffset, s.replicationOffset, commandBytes)
			logInfo("REPLICA", "Received %s, offset now: %d", cmd, s.replicationOffset)
		}
	}
}

func expectSimpleString(reader *bufio.Reader, expected string) {
	line, err := reader.ReadString('\n')
	if err != nil {
		logError("HANDSHAKE", "Failed to read line: %v", err)
		log.Fatalf("expectSimpleString failed to read line: %v", err)
	}

	line = strings.TrimSpace(line)
	logNetwork("HANDSHAKE", "IN", "Received: %s", line)

	if !strings.HasPrefix(line, "+") {
		logError("HANDSHAKE", "Expected simple string but got: %s", line)
		log.Fatalf("expectSimpleString expected a simple string but got: %s", line)
	}

	actual := strings.TrimPrefix(line, "+")
	if actual != expected {
		logError("HANDSHAKE", "Expected +%s, got: %s", expected, actual)
		log.Fatalf("Expected +%s, got: %s", expected, actual)
	}

	logSuccess("HANDSHAKE", "Received expected response: +%s", expected)
}

func (s *Server) removeReplica(conn net.Conn) {
	logInfo("REPLICA", "Removing replica connection: %s", conn.RemoteAddr())

	// Remove from replica connections
	for i, rconn := range s.replicaConn {
		if rconn == conn {
			s.replicaConn = append(s.replicaConn[:i], s.replicaConn[i+1:]...)
			logDebug("REPLICA", "Removed from replica connections list")
			break
		}
	}

	// Remove from replica offsets
	delete(s.replicaOffsets, conn)
	logDebug("REPLICA", "Removed from replica offsets map")

	logSuccess("REPLICA", "Replica removed successfully: %s", conn.RemoteAddr())
}
