package commands

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"
	"github.com/r0ld3x/redis-clone-go/app/internal/server"

	"github.com/r0ld3x/redis-clone-go/app/pkg/database"
)

// MultiHandler handles MULTI commands
type MultiHandler struct {
	logger *logging.Logger
}

func (h *MultiHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("MULTI")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if srv.TransactionMgr.IsInTransaction(clientConn) {
		protocol.WriteError(clientConn, "MULTI calls can not be nested")
		return nil
	}

	srv.TransactionMgr.StartTransaction(clientConn)
	protocol.WriteSimpleString(clientConn, "OK")
	h.logger.Success("Command completed successfully")
	return nil
}

// ExecHandler handles EXEC commands
type ExecHandler struct {
	logger *logging.Logger
}

func (h *ExecHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("EXEC")
	}

	if !srv.TransactionMgr.IsInTransaction(clientConn) {
		protocol.WriteError(clientConn, "ERR EXEC without MULTI")
		return nil
	}

	queuedCommands := srv.TransactionMgr.GetQueuedCommands(clientConn)
	results := make([]string, 0)

	for _, queuedCmd := range queuedCommands {
		fmt.Printf("queuedCmd: %+v\n", queuedCmd)
		result := h.executeCommand(srv, clientConn, queuedCmd.Command, queuedCmd.Args)
		log.Printf("%+v\n", result)
		results = append(results, result)
	}

	protocol.WriteArray2(clientConn, results)
	srv.TransactionMgr.EndTransaction(clientConn)
	return nil
}

func (h *ExecHandler) executeCommand(srv *server.Server, clientConn net.Conn, cmd string, args []string) string {
	switch strings.ToUpper(cmd) {
	case "SET":
		return h.executeSetCommand(srv, clientConn, args)
	case "GET":
		return h.executeGetCommand(srv, clientConn, args)
	case "ECHO":
		return h.executeEchoCommand(srv, clientConn, args)
	case "PING":
		return h.executePingCommand(srv, clientConn, args)
	case "INCR":
		return h.executeIncrCommand(srv, clientConn, args)
	default:
		return protocol.FormatError("ERR unknown command '" + cmd + "'")
	}
}

func (h *ExecHandler) executeSetCommand(srv *server.Server, clientConn net.Conn, args []string) string {
	if len(args) < 2 {
		return protocol.FormatError("ERR wrong number of arguments for 'SET'")
	}

	key, val := args[0], args[1]
	ms := -1
	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		ms, _ = strconv.Atoi(args[3])
	}

	database.SetKey(key, val, ms)

	if srv.IsMaster() {
		command := []string{"SET", key, val}
		if ms > -1 {
			command = append(command, "PX", strconv.Itoa(ms))
		}
		srv.ReplicateCommand(command)
	}

	return "+OK\r\n"
}

func (h *ExecHandler) executeGetCommand(srv *server.Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return protocol.FormatError("ERR wrong number of arguments for 'GET'")
	}

	key := args[0]
	val, success := database.GetKey(key)
	if !success {
		return "$-1\r\n"
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
}

func (h *ExecHandler) executeIncrCommand(srv *server.Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return protocol.FormatError("ERR wrong number of arguments for 'INCR'")
	}

	key := args[0]
	resp, success := database.Increment(key, 1)
	if !success {
		return protocol.FormatError("ERR value is not an integer or out of range")
	}

	receivedInt, err := strconv.Atoi(resp)
	if err != nil {
		return protocol.FormatError("ERR value is not an integer or out of range")
	}

	return fmt.Sprintf(":%d\r\n", receivedInt)
}

func (h *ExecHandler) executeEchoCommand(srv *server.Server, clientConn net.Conn, args []string) string {
	if len(args) < 1 {
		return protocol.FormatError("ERR wrong number of arguments for 'ECHO'")
	}

	return protocol.FormatBulkString(args[0])
}

func (h *ExecHandler) executePingCommand(srv *server.Server, clientConn net.Conn, args []string) string {
	if len(args) == 0 {
		return protocol.FormatSimpleString("PONG")
	}
	return protocol.FormatBulkString(args[0])
}

// DiscardHandler handles DISCARD commands
type DiscardHandler struct {
	logger *logging.Logger
}

func (h *DiscardHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("DISCARD")
	}

	if !srv.TransactionMgr.IsInTransaction(clientConn) {
		protocol.WriteError(clientConn, "ERR DISCARD without MULTI")
		return nil
	}

	srv.TransactionMgr.DiscardTransaction(clientConn)
	protocol.WriteSimpleString(clientConn, "OK")
	return nil
}
