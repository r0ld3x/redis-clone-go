package server

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/r0ld3x/redis-clone-go/app/internal/config"
	"github.com/r0ld3x/redis-clone-go/app/internal/logging"
	"github.com/r0ld3x/redis-clone-go/app/internal/protocol"

	"github.com/r0ld3x/redis-clone-go/app/internal/transaction"
)

type Server struct {
	Config            *config.Config
	ReplicaConn       []net.Conn
	MasterConn        net.Conn
	ReplicationOffset int
	ReplicationID     string
	ReplicaOffsets    map[net.Conn]int
	AckReceived       chan net.Conn
	HandshakeComplete bool
	TransactionMgr    *transaction.Manager
	Logger            *logging.Logger
	Mutex             sync.RWMutex
}

func NewServer(cfg *config.Config) *Server {
	return &Server{
		Config:            cfg,
		ReplicaOffsets:    make(map[net.Conn]int),
		ReplicationID:     generateReplID(),
		ReplicationOffset: 0,
		AckReceived:       make(chan net.Conn, 100),
		TransactionMgr:    transaction.NewManager(),
		Logger:            logging.NewLogger("SERVER"),
	}
}

func (s *Server) IsMaster() bool {
	return s.Config.IsMaster()
}

func (s *Server) IsSlave() bool {
	return s.Config.IsSlave()
}

func (s *Server) AddReplica(conn net.Conn) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.ReplicaConn = append(s.ReplicaConn, conn)
	s.ReplicaOffsets[conn] = 0
	s.Logger.Debug("Added replica to connections list. Total replicas: %d", len(s.ReplicaConn))
}

func (s *Server) RemoveReplica(conn net.Conn) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.Logger.Info("Removing replica connection: %s", conn.RemoteAddr())

	for i, rconn := range s.ReplicaConn {
		if rconn == conn {
			s.ReplicaConn = append(s.ReplicaConn[:i], s.ReplicaConn[i+1:]...)
			s.Logger.Debug("Removed from replica connections list")
			break
		}
	}

	delete(s.ReplicaOffsets, conn)
	s.Logger.Debug("Removed from replica offsets map")
	s.Logger.Success("Replica removed successfully: %s", conn.RemoteAddr())
}

func (s *Server) UpdateReplicationOffset(bytes int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	oldOffset := s.ReplicationOffset
	s.ReplicationOffset += bytes
	s.Logger.Debug("Updated master replication offset: %d -> %d (+%d bytes)",
		oldOffset, s.ReplicationOffset, bytes)
}

func (s *Server) UpdateReplicaOffset(conn net.Conn, offset int) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	s.ReplicaOffsets[conn] = offset
}

func (s *Server) GetReplicaOffset(conn net.Conn) int {
	s.Mutex.RLock()
	defer s.Mutex.RUnlock()

	offset, exists := s.ReplicaOffsets[conn]
	if !exists {
		return 0
	}
	return offset
}

func (s *Server) ReplicateCommand(command []string) {
	if !s.IsMaster() {
		return
	}

	encoded := protocol.EncodeArray(command)
	s.UpdateReplicationOffset(len(encoded))

	s.Mutex.RLock()
	replicas := make([]net.Conn, len(s.ReplicaConn))
	copy(replicas, s.ReplicaConn)
	s.Mutex.RUnlock()

	s.Logger.Info("Replicating to %d replicas", len(replicas))

	for _, conn := range replicas {
		s.Logger.Network("OUT", "Sending command to replica %s", conn.RemoteAddr())

		bytesWritten, err := conn.Write([]byte(encoded))
		if err != nil {
			s.Logger.Error("Replica %s disconnected: %v", conn.RemoteAddr(), err)
			s.RemoveReplica(conn)
			continue
		}

		s.Mutex.Lock()
		oldReplicaOffset := s.ReplicaOffsets[conn]
		s.ReplicaOffsets[conn] += bytesWritten
		s.Logger.Debug("Updated replica %s offset: %d -> %d (+%d bytes)",
			conn.RemoteAddr(), oldReplicaOffset, s.ReplicaOffsets[conn], bytesWritten)
		s.Mutex.Unlock()
	}
}

func (s *Server) IsConnectionClosed(conn net.Conn) bool {
	// Try to read one byte with immediate timeout
	one := make([]byte, 1)
	conn.SetReadDeadline(time.Now())
	if _, err := conn.Read(one); err == io.EOF {
		s.Logger.Debug("Connection closed detected for %s", conn.RemoteAddr())
		return true
	}
	// Reset deadline
	var zero time.Time
	conn.SetReadDeadline(zero)
	return false
}

func (s *Server) SendHandshake() error {
	s.Logger.Info("==================== HANDSHAKE START ====================")
	reader := bufio.NewReader(s.MasterConn)

	s.Logger.Info("Starting handshake with master %s", s.Config.MasterAddress)

	// Step 1: PING
	s.Logger.Network("OUT", "Sending PING to master")
	protocol.WriteArray(s.MasterConn, []string{"PING"})
	if err := s.expectSimpleString(reader, "PONG"); err != nil {
		return err
	}
	s.Logger.Success("PING handshake successful")

	// Step 2: REPLCONF listening-port
	s.Logger.Network("OUT", "Sending REPLCONF listening-port %s", s.Config.Port)
	protocol.WriteArray(s.MasterConn, []string{"REPLCONF", "listening-port", s.Config.Port})
	if err := s.expectSimpleString(reader, "OK"); err != nil {
		return err
	}
	s.Logger.Success("REPLCONF listening-port handshake successful")

	// Step 3: REPLCONF capa
	s.Logger.Network("OUT", "Sending REPLCONF capa psync2")
	protocol.WriteArray(s.MasterConn, []string{"REPLCONF", "capa", "psync2"})
	if err := s.expectSimpleString(reader, "OK"); err != nil {
		return err
	}
	s.Logger.Success("REPLCONF capa handshake successful")

	// Step 4: PSYNC
	s.Logger.Network("OUT", "Sending PSYNC ? -1")
	protocol.WriteArray(s.MasterConn, []string{"PSYNC", "?", "-1"})
	line, _ := reader.ReadString('\n')
	s.Logger.Network("IN", "PSYNC response: %s", strings.TrimSpace(line))

	if strings.HasPrefix(line, "+FULLRESYNC") {
		parts := strings.Split(strings.TrimSpace(line), " ")
		if len(parts) >= 3 {
			s.ReplicationID = parts[1]
			s.Logger.Debug("Set replication ID: %s", s.ReplicationID)
		}
	}

	// Read RDB file
	rdbHeader, _ := reader.ReadString('\n') // $<rdbLen>
	rdbLenStr := strings.TrimSpace(rdbHeader[1:])
	rdbLen, _ := strconv.Atoi(rdbLenStr)
	s.Logger.Info("Reading RDB file of %d bytes", rdbLen)

	// Skip RDB content
	io.CopyN(io.Discard, reader, int64(rdbLen))
	s.Logger.Debug("RDB file content skipped")

	s.HandshakeComplete = true
	s.Logger.Success("PSYNC handshake successful")

	// Reset replication offset after handshake
	s.ReplicationOffset = 0
	s.Logger.Info("Reset replication offset to 0 after handshake")
	s.Logger.Info("==================== HANDSHAKE END ====================")

	return nil
}

func (s *Server) expectSimpleString(reader *bufio.Reader, expected string) error {
	line, err := reader.ReadString('\n')
	if err != nil {
		s.Logger.Error("Failed to read line: %v", err)
		return fmt.Errorf("expectSimpleString failed to read line: %v", err)
	}

	line = strings.TrimSpace(line)
	s.Logger.Network("IN", "Received: %s", line)

	if !strings.HasPrefix(line, "+") {
		s.Logger.Error("Expected simple string but got: %s", line)
		return fmt.Errorf("expectSimpleString expected a simple string but got: %s", line)
	}

	actual := strings.TrimPrefix(line, "+")
	if actual != expected {
		s.Logger.Error("Expected +%s, got: %s", expected, actual)
		return fmt.Errorf("expected +%s, got: %s", expected, actual)
	}

	s.Logger.Success("Received expected response: +%s", expected)
	return nil
}

func (s *Server) SendFullResync(clientConn net.Conn) error {
	fullresyncResp := fmt.Sprintf("FULLRESYNC %s %d", s.ReplicationID, s.ReplicationOffset)
	s.Logger.Network("OUT", "Sending FULLRESYNC response: %s", fullresyncResp)
	protocol.WriteSimpleString(clientConn, fullresyncResp)

	// Send empty RDB file
	rdb := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(rdb)))
	n, err := base64.StdEncoding.Decode(dst, []byte(rdb))
	if err != nil {
		s.Logger.Error("Failed to decode base64 RDB: %v", err)
		return err
	}
	dst = dst[:n]

	s.Logger.Network("OUT", "Sending RDB file (%d bytes)", len(dst))
	clientConn.Write([]byte(fmt.Sprintf("$%v\r\n", len(dst))))
	clientConn.Write(dst)
	s.Logger.Success("FULLRESYNC completed for %s", clientConn.RemoteAddr())

	return nil
}

// generateReplID generates a random replication ID
func generateReplID() string {
	chars := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	result := make([]byte, 40)
	for i := range result {
		result[i] = chars[i%len(chars)] // Simple deterministic generation for now
	}
	return string(result)
}
