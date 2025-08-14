# Redis Server Implementation

This is a refactored Redis server implementation in Go, following Go best practices for project structure and code organization.

## Project Structure

```
app/
├── main.go                  # Main server application
├── internal/               # Private application code
│   ├── commands/          # Command handlers
│   │   ├── interface.go   # Command interface and registry
│   │   ├── basic.go       # Basic commands (PING, ECHO, COMMAND)
│   │   ├── data.go        # Data commands (GET, SET, INCR, KEYS, TYPE)
│   │   ├── server.go      # Server commands (CONFIG, INFO, REPLCONF, PSYNC, WAIT)
│   │   ├── transaction.go # Transaction commands (MULTI, EXEC, DISCARD)
│   │   └── stream.go      # Stream commands (XADD, XRANGE, XREAD)
│   ├── config/            # Configuration management
│   │   └── config.go      # Configuration loading and validation
│   ├── logging/           # Centralized logging
│   │   └── logger.go      # Logger implementation
│   ├── protocol/          # RESP protocol handling
│   │   └── resp.go        # RESP protocol read/write functions
│   ├── server/            # Server core logic
│   │   └── server.go      # Server struct and methods
│   └── transaction/       # Transaction handling
│       └── transaction.go # Transaction manager
└── pkg/                   # Public packages
    ├── database/          # Database operations
    │   ├── database.go    # Core database operations
    │   └── stream.go      # Stream data structure operations
    └── rdb/              # RDB file parsing
        ├── parser.go      # RDB file parser
        └── helpers.go     # RDB parsing helpers
```

## Key Improvements

### 1. **Proper Separation of Concerns**

- **Commands**: Each command type is in its own file with focused responsibility
- **Server Logic**: Separated from command handling
- **Protocol**: RESP protocol handling is isolated
- **Configuration**: Centralized configuration management
- **Logging**: Consistent logging across all components

### 2. **Better Code Organization**

- **cmd/**: Application entry points following Go conventions
- **internal/**: Private application code that can't be imported by other projects
- **pkg/**: Public packages that could be reused by other projects

### 3. **Improved Error Handling**

- Consistent error handling patterns across all components
- Proper error propagation and logging
- Graceful handling of connection failures

### 4. **Enhanced Maintainability**

- Single responsibility principle applied to all modules
- Clear interfaces and abstractions
- Reduced code duplication
- Better testability through dependency injection

### 5. **Thread Safety**

- Proper mutex usage for concurrent access
- Safe replica management
- Transaction isolation per connection

## Running the Server

```bash
# From the project root
go run app/main.go [flags]

# Available flags:
# --port=6379              # Port to listen on
# --dir=/path/to/data      # Data directory
# --dbfilename=dump.rdb    # RDB filename
# --replicaof="host port"  # Master address for replica mode
```

## Supported Commands

### Basic Commands

- `PING` - Test connectivity
- `ECHO <message>` - Echo a message
- `COMMAND` - Get command info

### Data Commands

- `GET <key>` - Get value by key
- `SET <key> <value> [PX <milliseconds>]` - Set key-value with optional TTL
- `INCR <key>` - Increment integer value
- `KEYS <pattern>` - Find keys matching pattern
- `TYPE <key>` - Get key type

### Server Commands

- `CONFIG GET <parameter>` - Get configuration parameter
- `INFO [section]` - Get server information
- `REPLCONF <subcommand> [args...]` - Replication configuration
- `PSYNC <replid> <offset>` - Partial synchronization
- `WAIT <numreplicas> <timeout>` - Wait for replica acknowledgments

### Transaction Commands

- `MULTI` - Start transaction
- `EXEC` - Execute transaction
- `DISCARD` - Discard transaction

### Stream Commands

- `XADD <key> <id> <field> <value> [field value ...]` - Add entry to stream
- `XRANGE <key> <start> <end>` - Get range of entries from stream
- `XREAD [BLOCK <milliseconds>] STREAMS <key> [key ...] <id> [id ...]` - Read from streams

## Architecture Features

### Master-Slave Replication

- Automatic handshake process
- Command replication to slaves
- Offset tracking and synchronization
- RDB file transfer for full resync

### Transaction Support

- ACID transaction properties
- Command queuing during MULTI
- Atomic execution with EXEC
- Transaction rollback with DISCARD

### Stream Data Structure

- Time-ordered entries with unique IDs
- Range queries and blocking reads
- Automatic ID generation
- Field-value pair storage

### RDB Persistence

- RDB file parsing and loading
- Support for expiration times
- Metadata and database selection
- Various encoding formats

## Code Quality Features

- **Comprehensive logging** with different log levels
- **Proper error handling** with detailed error messages
- **Thread-safe operations** with appropriate locking
- **Clean interfaces** for easy testing and extension
- **Consistent code style** following Go conventions
- **Modular design** for easy maintenance and updates
