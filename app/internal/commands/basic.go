package commands

import (
	"net"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"
	"github.com/r0ld3x/redis-clone-go/app/internal/server"
)

// PingHandler handles PING commands
type PingHandler struct {
	logger *logging.Logger
}

func (h *PingHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("PING")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)
	h.logger.Network("OUT", "Sending PONG response")
	protocol.WriteSimpleString(clientConn, "PONG")
	h.logger.Success("Command completed successfully")
	return nil
}

// EchoHandler handles ECHO commands
type EchoHandler struct {
	logger *logging.Logger
}

func (h *EchoHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("ECHO")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'ECHO'")
		return nil
	}

	h.logger.Network("OUT", "Sending bulk string response: %s", args[0])
	protocol.WriteBulkString(clientConn, args[0])
	h.logger.Success("Command completed successfully")
	return nil
}

// CommandHandler handles COMMAND commands
type CommandHandler struct {
	logger *logging.Logger
}

func (h *CommandHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("COMMAND")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)
	h.logger.Network("OUT", "Sending OK response")
	protocol.WriteSimpleString(clientConn, "OK")
	h.logger.Success("Command completed successfully")
	return nil
}
