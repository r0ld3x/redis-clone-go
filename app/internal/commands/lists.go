package commands

import (
	"net"
	"strconv"
	"time"

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

type LLenHandler struct {
	logger *logging.Logger
}

func (h *LLenHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("LLEN")
	}

	if len(args) > 1 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'LLEN' command")
		return nil
	}

	key := args[0]
	data, err := database.GetArrayLength(key)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return nil
	}
	// command := append([]string{"RPUSH", strconv.Itoa(start), strconv.Itoa(end)})
	// srv.ReplicateCommand(command)

	protocol.WriteInteger(clientConn, data)
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

type LPopHandler struct {
	logger *logging.Logger
}

func (h *LPopHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("LPOP")
	}

	if len(args) < 1 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'LPOP' command")
		return nil
	}

	key := args[0]
	var n = 0
	if len(args) >= 2 {
		number, err := strconv.Atoi(args[1])
		if err != nil {
			protocol.WriteError(clientConn, "ERR wrong number of arguments for 'LPOP' command")
			return nil
		}
		n = number
		data, err := database.RemoveNFromArray(key, n)
		if err != nil {
			protocol.WriteError(clientConn, err.Error())
			return nil
		}
		protocol.WriteArray(clientConn, data)
		return nil
	}
	data, err := database.RemoveNFromArray(key, 0)
	if err != nil {
		protocol.WriteError(clientConn, err.Error())
		return nil
	}

	// command := append([]string{"RPUSH", strconv.Itoa(start), strconv.Itoa(end)})
	// srv.ReplicateCommand(command)

	if len(data) == 0 {
		clientConn.Write([]byte("$-1\r\n"))
		return nil
	}

	str := ""
	for _, v := range data {
		str += v
	}
	protocol.WriteBulkString(clientConn, str)
	return nil
}

type BLPopHandler struct {
	logger *logging.Logger
}

func (h *BLPopHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("BLPOP")
	}

	if len(args) < 2 {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'BLPOP' command")
		return nil
	}

	key := args[0]
	timeout, err := strconv.Atoi(args[1])
	if err != nil {
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'BLPOP' command")
		return nil
	}
	req := database.BlpopRequest{
		ListName:   key,
		ResultChan: make(chan []string, 1),
		Timeout:    time.Duration(timeout) * time.Second,
	}
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		startTime := time.Now()
		for {
			val, found := database.DB.Load(req.ListName)
			if found {
				if slice, ok := val.([]string); ok && len(slice) > 0 {
					element := slice
					newSlice := slice[1:]
					database.DB.Store(req.ListName, newSlice)
					req.ResultChan <- element
					return
				}
			}

			if timeout != 0 && time.Since(startTime) > req.Timeout {
				// Timeout reached, return
				req.ResultChan <- []string{}
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	result := <-req.ResultChan
	if result == nil {
		clientConn.Write([]byte("$-1\r\n"))
		return nil
	}
	combined := append([]string{req.ListName}, result...)
	protocol.WriteArray(clientConn, combined)
	return nil
}
