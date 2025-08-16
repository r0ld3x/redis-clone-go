package commands

import (
	"net"
	"strconv"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"
	"github.com/r0ld3x/redis-clone-go/app/internal/server"
	"github.com/r0ld3x/redis-clone-go/app/pkg/database"
)

// RPushHandler handles RPUSH commands
type RPushHandler struct {
	logger *logging.Logger
}

func (h *RPushHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("RPUSH")
	}

	if len(args) < 2 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'RPUSH' command")
		return nil
	}

	key := args[0]
	values := args[1:]

	totalLength := 0
	for _, item := range values {
		len, err := database.RPushAdd(key, item)
		if err != nil {
			protocol.WriteError(clientConn, err.Error())
			return nil
		}
		totalLength = len
	}

	// command := append([]string{"RPUSH", key}, values...)
	// srv.ReplicateCommand(command)

	protocol.WriteInteger(clientConn, totalLength)
	return nil
}

// LPushHandler handles LPUSH commands
type LPushHandler struct {
	logger *logging.Logger
}

func (h *LPushHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("LPUSH")
	}

	if len(args) < 2 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'LPUSH' command")
		return nil
	}

	key := args[0]
	values := args[1:]

	totalLength := 0
	for _, item := range values {
		len, err := database.LPush(key, item)
		if err != nil {
			protocol.WriteError(clientConn, err.Error())
			return nil
		}
		totalLength = len
	}

	// command := append([]string{"RPUSH", key}, values...)
	// srv.ReplicateCommand(command)

	protocol.WriteInteger(clientConn, totalLength)
	return nil
}

type LRangeHandler struct {
	logger *logging.Logger
}

func (h *LRangeHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("LRANGE")
	}

	if len(args) != 3 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'LRANGE' command")
		return nil
	}

	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		protocol.WriteError(clientConn, "ARGS should be a integer")
		return nil
	}
	end, err := strconv.Atoi(args[2])
	if err != nil {
		protocol.WriteError(clientConn, "ARGS should be a integer")
		return nil
	}
	data, err := database.LRange(key, start, end)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return nil
	}
	// command := append([]string{"RPUSH", strconv.Itoa(start), strconv.Itoa(end)})
	// srv.ReplicateCommand(command)

	protocol.WriteArray(clientConn, data)
	return nil
}
