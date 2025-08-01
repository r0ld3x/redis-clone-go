package commands

import (
	"net"
	"path"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/logging"
	"github.com/codecrafters-io/redis-starter-go/app/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/database"
)

// GetHandler handles GET commands
type GetHandler struct {
	logger *logging.Logger
}

func (h *GetHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("GET")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'GET'")
		return nil
	}

	key := args[0]
	h.logger.Debug("Looking up key: %s", key)

	val, success := database.GetKey(key)
	if !success {
		h.logger.Info("Key not found: %s", key)
		h.logger.Network("OUT", "Sending null response")
		clientConn.Write([]byte("$-1\r\n"))
		return nil
	}

	h.logger.Info("Key found: %s = %s", key, val)
	h.logger.Network("OUT", "Sending value: %s", val)
	protocol.WriteSimpleString(clientConn, val)
	h.logger.Success("Command completed successfully")
	return nil
}

// SetHandler handles SET commands
type SetHandler struct {
	logger *logging.Logger
}

func (h *SetHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("SET")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 2 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'SET'")
		return nil
	}

	if !srv.IsMaster() {
		h.logger.Error("Attempted write on replica from %s", clientConn.RemoteAddr())
		protocol.WriteError(clientConn, "READONLY You can't write against a read only replica.")
		return nil
	}

	key, val := args[0], args[1]
	ms := -1
	if len(args) == 4 && strings.ToUpper(args[2]) == "PX" {
		ms, _ = strconv.Atoi(args[3])
	}

	h.logger.Debug("Storing key=%s value=%s TTL(ms)=%d", key, val, ms)
	database.SetKey(key, val, ms)
	h.logger.Info("Key stored successfully: %s = %s", key, val)

	// Build the full SET command for replication
	command := []string{"SET", key, val}
	if ms > -1 {
		command = append(command, "PX", strconv.Itoa(ms))
	}

	// Replicate to slaves
	srv.ReplicateCommand(command)

	// Respond to client
	h.logger.Network("OUT", "Sending OK response to client")
	protocol.WriteSimpleString(clientConn, "OK")
	h.logger.Success("Command completed successfully")
	return nil
}

// IncrHandler handles INCR commands
type IncrHandler struct {
	logger *logging.Logger
}

func (h *IncrHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("INCR")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'INCR'")
		return nil
	}

	key := args[0]
	by := 1
	if len(args) > 1 {
		if intVal, err := strconv.Atoi(args[1]); err == nil {
			by = intVal
		}
	}

	resp, success := database.Increment(key, by)
	if !success {
		protocol.WriteError(clientConn, "ERR value is not an integer or out of range")
		return nil
	}

	receivedInt, err := strconv.Atoi(resp)
	if err != nil {
		protocol.WriteError(clientConn, "failed to convert response to integer")
		return nil
	}

	protocol.WriteInteger(clientConn, receivedInt)
	h.logger.Success("Command completed successfully")
	return nil
}

// KeysHandler handles KEYS commands
type KeysHandler struct {
	logger *logging.Logger
}

func (h *KeysHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("KEYS")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'KEYS'")
		return nil
	}

	pattern := args[0]
	h.logger.Debug("Searching for pattern: %s", pattern)

	data := h.getKeysMatchingPattern(pattern)
	h.logger.Info("Found %d keys matching pattern %s", len(data), pattern)

	h.logger.Network("OUT", "Sending array response with %d keys", len(data))
	protocol.WriteArray(clientConn, data)
	h.logger.Success("Command completed successfully")
	return nil
}

func (h *KeysHandler) getKeysMatchingPattern(pattern string) []string {
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

// TypeHandler handles TYPE commands
type TypeHandler struct {
	logger *logging.Logger
}

func (h *TypeHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("TYPE")
	}

	if len(args) < 1 {
		protocol.WriteError(clientConn, "ERR no key provided")
		return nil
	}

	key := args[0]
	response, found := database.GetType(key)
	if !found {
		protocol.WriteSimpleString(clientConn, "none")
		return nil
	}

	protocol.WriteSimpleString(clientConn, response)
	return nil
}
