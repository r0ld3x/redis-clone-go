package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/logging"
	"github.com/codecrafters-io/redis-starter-go/app/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
)

// ConfigHandler handles CONFIG commands
type ConfigHandler struct {
	logger *logging.Logger
}

func (h *ConfigHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("CONFIG")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 2 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'CONFIG'")
		return nil
	}

	cmd, name := strings.ToUpper(args[0]), strings.ToUpper(args[1])
	h.logger.Debug("Processing subcommand: %s %s", cmd, name)

	if cmd == "GET" {
		switch name {
		case "DIR":
			h.logger.Info("Returning directory: %s", srv.Config.Directory)
			protocol.WriteArray(clientConn, []string{"dir", srv.Config.Directory})
		case "DBFILENAME":
			h.logger.Info("Returning DB filename: %s", srv.Config.DBFileName)
			protocol.WriteArray(clientConn, []string{"dbfilename", srv.Config.DBFileName})
		default:
			h.logger.Error("Unsupported parameter: %s", name)
			protocol.WriteError(clientConn, "unsupported CONFIG parameter")
		}
	}
	h.logger.Success("Command completed successfully")
	return nil
}

// InfoHandler handles INFO commands
type InfoHandler struct {
	logger *logging.Logger
}

func (h *InfoHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("INFO")
	}

	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	info := "# Replication\n"
	info += fmt.Sprintf("role:%s\r\n", srv.Config.Role)

	if srv.Config.Role == "slave" {
		info += fmt.Sprintf("master_host:%s\r\n", srv.Config.HostName)
		info += fmt.Sprintf("master_port:%s\r\n", srv.Config.Port)
	}
	info += fmt.Sprintf("master_replid:%s\r\n", srv.ReplicationID)
	info += fmt.Sprintf("master_repl_offset:%d\r\n", srv.ReplicationOffset)

	h.logger.Debug("Generated info response: %s", strings.ReplaceAll(info, "\r\n", "\\r\\n"))
	h.logger.Network("OUT", "Sending bulk string response")
	protocol.WriteBulkString(clientConn, info)
	h.logger.Success("Command completed successfully")
	return nil
}

// ReplconfHandler handles REPLCONF commands
type ReplconfHandler struct {
	logger *logging.Logger
}

func (h *ReplconfHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("REPLCONF")
	}

	h.logger.Info("==================== REPLCONF COMMAND START ====================")
	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 1 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'REPLCONF'")
		return nil
	}

	subcommand := strings.ToUpper(args[0])
	h.logger.Debug("Processing subcommand: %s", subcommand)

	switch subcommand {
	case "LISTENING-PORT":
		h.logger.Info("Handling LISTENING-PORT from %s", clientConn.RemoteAddr())
		h.logger.Network("OUT", "Sending OK response for LISTENING-PORT")
		protocol.WriteSimpleString(clientConn, "OK")
		h.logger.Success("LISTENING-PORT handled successfully")

	case "GETACK":
		h.logger.Info("Handling GETACK request from %s", clientConn.RemoteAddr())

		// For replicas, send their local offset
		if srv.IsSlave() {
			h.logger.Debug("Replica responding with local offset: %d", srv.ReplicationOffset)
			h.logger.Network("OUT", "Sending ACK with offset %d", srv.ReplicationOffset)
			protocol.WriteArray(clientConn, []string{"REPLCONF", "ACK", strconv.Itoa(srv.ReplicationOffset)})
			h.logger.Success("Sent ACK with replica offset %d", srv.ReplicationOffset)
		} else {
			// For master handling replica's GETACK, send replica's offset
			offset := srv.GetReplicaOffset(clientConn)
			h.logger.Debug("Master responding with replica offset: %d", offset)
			h.logger.Network("OUT", "Sending ACK with offset %d", offset)
			protocol.WriteArray(clientConn, []string{"REPLCONF", "ACK", strconv.Itoa(offset)})
			h.logger.Success("Sent ACK with offset %d", offset)
		}

	case "ACK":
		if len(args) >= 2 {
			offset, err := strconv.Atoi(args[1])
			if err == nil {
				srv.UpdateReplicaOffset(clientConn, offset)
				h.logger.Debug("Updated replica offset: %s -> %d", clientConn.RemoteAddr(), offset)

				select {
				case srv.AckReceived <- clientConn:
					h.logger.Debug("Successfully signaled ACK to WAIT command")
				default:
					h.logger.Debug("ACK channel full")
				}
			}
		}

	case "CAPA":
		h.logger.Info("Handling CAPA from %s", clientConn.RemoteAddr())
		h.logger.Network("OUT", "Sending OK response for CAPA")
		protocol.WriteSimpleString(clientConn, "OK")
		h.logger.Success("CAPA handled successfully")

	default:
		h.logger.Info("Handling unknown subcommand '%s' from %s", subcommand, clientConn.RemoteAddr())
		h.logger.Network("OUT", "Sending OK response for unknown subcommand")
		protocol.WriteSimpleString(clientConn, "OK")
		h.logger.Success("Unknown subcommand handled with OK")
	}

	h.logger.Info("==================== REPLCONF COMMAND END ====================")
	return nil
}

// PsyncHandler handles PSYNC commands
type PsyncHandler struct {
	logger *logging.Logger
}

func (h *PsyncHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("PSYNC")
	}

	h.logger.Info("==================== PSYNC COMMAND START ====================")
	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) != 2 {
		h.logger.Error("Invalid argument count: %d", len(args))
		protocol.WriteError(clientConn, "ERR invalid PSYNC arguments")
		return nil
	}

	replID := args[0]
	offset := args[1]
	h.logger.Debug("Replication ID: %s, Offset: %s", replID, offset)

	if replID == "?" && offset == "-1" {
		h.logger.Info("Performing FULLRESYNC for %s", clientConn.RemoteAddr())

		// Add to replica connections
		srv.AddReplica(clientConn)

		if err := srv.SendFullResync(clientConn); err != nil {
			return err
		}
	} else {
		// Partial resync
		h.logger.Info("Attempting partial resync with replID=%s offset=%s", replID, offset)
		h.logger.Network("OUT", "Sending CONTINUE response")
		protocol.WriteSimpleString(clientConn, "CONTINUE")

		// Ensure replica is in the list if not already
		srv.AddReplica(clientConn)
		h.logger.Success("Partial resync setup completed")
	}

	h.logger.Info("==================== PSYNC COMMAND END ====================")
	return nil
}

// WaitHandler handles WAIT commands
type WaitHandler struct {
	logger *logging.Logger
}

func (h *WaitHandler) Handle(srv *server.Server, clientConn net.Conn, args []string) error {
	if h.logger == nil {
		h.logger = logging.NewLogger("WAIT")
	}

	h.logger.Info("==================== WAIT COMMAND START ====================")
	h.logger.Info("Command received from %s with args: %v", clientConn.RemoteAddr(), args)

	if len(args) < 2 {
		h.logger.Error("Wrong number of arguments: %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'WAIT'")
		return nil
	}

	count, err1 := strconv.Atoi(args[0])
	timeout, err2 := strconv.Atoi(args[1])

	if err1 != nil || err2 != nil {
		h.logger.Error("Failed to parse arguments: count_err=%v, timeout_err=%v", err1, err2)
		protocol.WriteError(clientConn, "invalid arguments for 'WAIT'")
		return nil
	}

	h.logger.Info("Need %d acks within %d ms", count, timeout)

	srv.Mutex.RLock()
	replicaCount := len(srv.ReplicaConn)
	srv.Mutex.RUnlock()

	h.logger.Info("Connected replicas: %d", replicaCount)

	if replicaCount == 0 {
		h.logger.Info("No replicas, returning 0")
		protocol.WriteInteger(clientConn, 0)
		return nil
	}

	// Send GETACK to all replicas
	srv.Mutex.RLock()
	replicas := make([]net.Conn, len(srv.ReplicaConn))
	copy(replicas, srv.ReplicaConn)
	srv.Mutex.RUnlock()

	for _, conn := range replicas {
		go func(conn net.Conn) {
			conn.Write([]byte(protocol.EncodeArray([]string{"REPLCONF", "GETACK", "*"})))
		}(conn)
	}

	// Count initial acks (replicas that are already up to date)
	acks := 0
	srv.Mutex.RLock()
	for _, conn := range srv.ReplicaConn {
		if srv.ReplicaOffsets[conn] <= 0 {
			acks++
		}
	}
	srv.Mutex.RUnlock()

	// Wait for additional acks or timeout
	// timer := time.After(time.Duration(timeout) * time.Millisecond)
	//
	// outer:
	// for acks < count {
	// 	select {
	// 	case <-srv.AckReceived:
	// 		acks++
	// 		h.logger.Info("ACK received, total: %d", acks)
	// 	case <-timer:
	// 		h.logger.Info("Timeout reached, total: %d", acks)
	// 		break outer
	// 	}
	// }

	h.logger.Info("Returning %d acks", acks)
	protocol.WriteInteger(clientConn, acks)
	h.logger.Info("==================== WAIT COMMAND END ====================")
	return nil
}
