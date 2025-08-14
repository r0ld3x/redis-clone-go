package commands

import (
	"net"

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
		protocol.WriteError(clientConn, "ERR wrong number of arguments for 'RPUSH'")
		return nil
	}

	key := args[0]
	item := args[1]

	arrayLength := database.RPushAdd(key, item)
	// if err != nil {
	// 	protocol.WriteError(clientConn, err.Error())
	// 	return nil
	// }
	command := []string{"RPUSH", key, item}

	srv.ReplicateCommand(command)
	protocol.WriteInteger(clientConn, arrayLength)
	return nil
}
