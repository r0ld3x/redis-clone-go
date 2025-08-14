package commands

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"

	"github.com/r0ld3x/redis-clone-go/app/internal/server"
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

		if srv.IsSlave() {
			h.logger.Debug("Replica responding with local offset: %d", srv.ReplicationOffset)
			h.logger.Network("OUT", "Sending ACK with offset %d", srv.ReplicationOffset)
			protocol.WriteArray(clientConn, []string{"REPLCONF", "ACK", strconv.Itoa(srv.ReplicationOffset)})
			h.logger.Success("Sent ACK with replica offset %d", srv.ReplicationOffset)
		} else {
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

	// Reject WAIT on replicas
	if srv.IsSlave() {
		h.logger.Info("WAIT command called on a replica — not allowed")
		protocol.WriteError(clientConn, "ERR WAIT cannot be used with replica instances.")
		return nil
	}

	h.logger.Info("========== WAIT COMMAND START ==========")
	h.logger.Info("From %s — Args: %v", clientConn.RemoteAddr(), args)

	// Argument check
	if len(args) < 2 {
		h.logger.Error("Wrong number of arguments: got %d", len(args))
		protocol.WriteError(clientConn, "wrong number of arguments for 'WAIT'")
		return nil
	}

	count, err1 := strconv.Atoi(args[0])
	timeout, err2 := strconv.Atoi(args[1])
	if err1 != nil || err2 != nil {
		h.logger.Error("Invalid WAIT args — countErr=%v timeoutErr=%v", err1, err2)
		protocol.WriteError(clientConn, "invalid arguments for 'WAIT'")
		return nil
	}

	// Read current master replication offset
	srv.Mutex.RLock()
	masterOffset := srv.ReplicationOffset
	replicaCount := len(srv.ReplicaConn)
	srv.Mutex.RUnlock()

	h.logger.Info("Need %d acks within %d ms. Master offset=%d", count, timeout, masterOffset)
	h.logger.Info("Connected replicas: %d", replicaCount)

	if replicaCount == 0 {
		h.logger.Info("No replicas — returning 0 immediately")
		protocol.WriteInteger(clientConn, 0)
		return nil
	}

	for _, conn := range srv.ReplicaConn {
		h.logger.Debug("Sending REPLCONF GETACK * to %v", conn.RemoteAddr())
		conn.Write([]byte(protocol.EncodeArray([]string{"REPLCONF", "GETACK", "*"})))
	}

	// acks := 0
	// srv.Mutex.RLock()
	// for _, conn := range srv.ReplicaConn {
	// 	ro := srv.ReplicaOffsets[conn]
	// 	h.logger.Debug("Replica %v offset=%d (master=%d)", conn.RemoteAddr(), ro, masterOffset)
	// 	if ro >= masterOffset {
	// 		acks++
	// 	}
	// }
	// srv.Mutex.RUnlock()
	acks := 0
	for _, conn := range srv.ReplicaConn {
		fmt.Println("s.replicaOffsets[conn] ", srv.ReplicaOffsets[conn])
		if srv.ReplicaOffsets[conn] <= 0 {
			acks++
		}
	}

	h.logger.Info("Initial ACKs: %d", acks)

	timer := time.After(time.Duration(timeout) * time.Millisecond)

outer:
	for acks < count {
		select {
		case <-srv.AckReceived:
			acks++
			h.logger.Info("New ACK received — total=%d / %d", acks, count)
		case <-timer:
			h.logger.Info("WAIT timeout — total=%d / %d", acks, count)
			break outer
		}
	}

	// 	deadline := time.After(time.Duration(timeout) * time.Millisecond)

	// outer:
	// 	for acks < count {
	// 		select {
	// 		case <-srv.AckReceived:
	// 			srv.Mutex.RLock()
	// 			newAcks := 0
	// 			for _, conn := range srv.ReplicaConn {
	// 				ro := srv.ReplicaOffsets[conn]
	// 				if ro >= masterOffset {
	// 					newAcks++
	// 				}
	// 			}
	// 			acks = newAcks
	// 			srv.Mutex.RUnlock()

	// 			h.logger.Info("New ACK received — total=%d / %d", acks, count)

	// 		case <-deadline:
	// 			h.logger.Info("WAIT timeout — total=%d / %d", acks, count)
	// 			break outer
	// 		}
	// 	}

	h.logger.Info("Returning %d acks", acks)
	h.logger.Info("========== WAIT COMMAND END ==========")
	protocol.WriteInteger(clientConn, acks)
	return nil
}
