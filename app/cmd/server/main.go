package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/commands"
	"github.com/codecrafters-io/redis-starter-go/app/internal/config"
	"github.com/codecrafters-io/redis-starter-go/app/internal/logging"
	"github.com/codecrafters-io/redis-starter-go/app/internal/protocol"
	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/database"
	"github.com/codecrafters-io/redis-starter-go/app/pkg/rdb"
)

func main() {
	logger := logging.NewLogger("MAIN")
	logger.Info("Starting Redis server...")

	// Initialize database
	database.Start()

	// Load configuration
	cfg := config.LoadConfig()
	logger.Info("Server configuration: %+v", cfg)

	// Create server instance
	srv := server.NewServer(cfg)

	// Set up command registry
	registry := commands.NewRegistry()
	registry.RegisterAllHandlers()

	// Connect to master if this is a replica
	if cfg.IsSlave() {
		logger.Info("Connecting to master at %s", cfg.MasterAddress)
		var err error
		srv.MasterConn, err = net.Dial("tcp", cfg.MasterAddress)
		if err != nil {
			log.Fatalf("couldn't connect to master at %s: %v", cfg.MasterAddress, err)
		}
		logger.Success("Connected to master successfully")
		go func() {
			if err := srv.SendHandshake(); err != nil {
				log.Fatalf("handshake failed: %v", err)
			}
			handleMasterConnection(srv, registry)
		}()
	}

	// Load RDB file if master and file specified
	if cfg.IsMaster() && cfg.DBFileName != "" {
		rdbPath := cfg.Directory + "/" + cfg.DBFileName
		logger.Info("Loading RDB file: %s", rdbPath)
		if err := rdb.ParseRDB(rdbPath); err != nil {
			logger.Error("Failed to load RDB file: %v", err)
		}
	}

	// Start listening for connections
	listenAddress := cfg.GetListenAddress()
	l, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", listenAddress, err)
	}
	defer l.Close()

	logger.Success("[%s] Server listening on %s", cfg.Role, listenAddress)

	// Accept connections
	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Error("Accept error: %v", err)
			continue
		}
		logger.Info("New connection established from: %s", conn.RemoteAddr())
		go handleClientConnection(srv, conn, registry)
	}
}

func handleClientConnection(srv *server.Server, conn net.Conn, registry *commands.Registry) {
	logger := logging.NewLogger("CONNECTION")
	logger.Info("Starting connection handler for %s", conn.RemoteAddr())

	defer func() {
		conn.Close()
		srv.RemoveReplica(conn)
		srv.TransactionMgr.CleanupConnection(conn)
	}()

	scanner := bufio.NewScanner(conn)

	for {
		if srv.IsConnectionClosed(conn) {
			logger.Info("Connection closed by client: %s", conn.RemoteAddr())
			return
		}

		logger.Debug("Waiting for command from %s", conn.RemoteAddr())
		args, ok := protocol.ReadArrayArguments(scanner, conn)
		if !ok {
			logger.Info("Connection closed or error reading from: %s", conn.RemoteAddr())
			return
		}

		logger.Network("IN", "Received command from %s: %v", conn.RemoteAddr(), args)

		if len(args) < 1 {
			logger.Error("Empty command received from %s", conn.RemoteAddr())
			protocol.WriteError(conn, "ERR parsing args")
			continue
		}

		cmd := strings.ToUpper(args[0])
		commandArgs := args[1:]

		// Handle transaction commands
		if srv.TransactionMgr.IsInTransaction(conn) {
			if cmd == "EXEC" || cmd == "DISCARD" || cmd == "MULTI" {
				handler, exists := registry.Get(commands.Command(cmd))
				if exists {
					handler.Handle(srv, conn, commandArgs)
				} else {
					protocol.WriteError(conn, "unknown command '"+cmd+"'")
				}
			} else {
				srv.TransactionMgr.QueueCommand(conn, cmd, commandArgs)
				protocol.WriteSimpleString(conn, "QUEUED")
			}
		} else {
			logger.Debug("Processing command: '%s' with args: %v", cmd, commandArgs)

			handler, exists := registry.Get(commands.Command(cmd))
			if exists {
				logger.Debug("Found handler for command: %s", cmd)
				if err := handler.Handle(srv, conn, commandArgs); err != nil {
					logger.Error("Handler error for command %s: %v", cmd, err)
					protocol.WriteError(conn, "ERR internal server error")
				}
			} else {
				logger.Error("No handler found for command: %s", cmd)
				protocol.WriteError(conn, "unknown command '"+cmd+"'")
			}
		}
	}
}

func handleMasterConnection(srv *server.Server, registry *commands.Registry) {
	logger := logging.NewLogger("REPLICA")
	logger.Info("Starting to handle commands from master")

	// Create a new scanner from the master connection after handshake
	scanner := bufio.NewScanner(srv.MasterConn)

	for {
		if srv.IsConnectionClosed(srv.MasterConn) {
			logger.Error("Connection to master lost")
			return
		}

		args, ok := protocol.ReadArrayArguments(scanner, srv.MasterConn)
		if !ok {
			logger.Error("Connection to master lost or error reading")
			return
		}

		logger.Network("IN", "Received command from master: %v", args)

		if len(args) == 0 {
			continue
		}

		commandBytes := len(protocol.EncodeArray(args))
		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			// Update offset for PING
			oldOffset := srv.ReplicationOffset
			srv.ReplicationOffset += commandBytes
			logger.Debug("Updated replication offset for PING: %d -> %d (+%d bytes)",
				oldOffset, srv.ReplicationOffset, commandBytes)
			logger.Info("Received PING from master, offset now: %d", srv.ReplicationOffset)

		case "SET":
			// Update offset for SET
			oldOffset := srv.ReplicationOffset
			srv.ReplicationOffset += commandBytes
			logger.Debug("Updated replication offset for SET: %d -> %d (+%d bytes)",
				oldOffset, srv.ReplicationOffset, commandBytes)

			if len(args) >= 3 {
				key := args[1]
				val := args[2]
				ms := -1
				if len(args) == 5 && strings.ToUpper(args[3]) == "PX" {
					ms, _ := fmt.Sscanf(args[4], "%d", &ms)
					_ = ms // Use the parsed value
				}
				database.SetKey(key, val, ms)
				logger.Info("Applied SET %s=%s (TTL: %d ms), offset now: %d", key, val, ms, srv.ReplicationOffset)
			}

		case "REPLCONF":
			if len(args) >= 2 {
				subcommand := strings.ToUpper(args[1])
				switch subcommand {
				case "GETACK":
					// CRITICAL: Respond with current offset BEFORE updating it
					logger.Info("Received GETACK, responding with ACK %d", srv.ReplicationOffset)
					logger.Network("OUT", "Sending ACK with offset %d", srv.ReplicationOffset)
					protocol.WriteArray(srv.MasterConn, []string{"REPLCONF", "ACK", fmt.Sprintf("%d", srv.ReplicationOffset)})

					// Update offset AFTER responding
					oldOffset := srv.ReplicationOffset
					srv.ReplicationOffset += commandBytes
					logger.Debug("Updated replication offset for GETACK: %d -> %d (+%d bytes)",
						oldOffset, srv.ReplicationOffset, commandBytes)
				default:
					// Update offset for other REPLCONF commands
					oldOffset := srv.ReplicationOffset
					srv.ReplicationOffset += commandBytes
					logger.Debug("Updated replication offset for REPLCONF %s: %d -> %d (+%d bytes)",
						subcommand, oldOffset, srv.ReplicationOffset, commandBytes)
					logger.Info("Received REPLCONF %s, offset now: %d", subcommand, srv.ReplicationOffset)
				}
			}

		default:
			// Update offset for any other commands
			oldOffset := srv.ReplicationOffset
			srv.ReplicationOffset += commandBytes
			logger.Debug("Updated replication offset for %s: %d -> %d (+%d bytes)",
				cmd, oldOffset, srv.ReplicationOffset, commandBytes)
			logger.Info("Received %s, offset now: %d", cmd, srv.ReplicationOffset)
		}
	}
}
