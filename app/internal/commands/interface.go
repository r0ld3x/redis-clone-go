package commands

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/internal/server"
)

// Command represents a Redis command type
type Command string

const (
	CommandCommand  Command = "COMMAND"
	EchoCommand     Command = "ECHO"
	PingCommand     Command = "PING"
	GetCommand      Command = "GET"
	SetCommand      Command = "SET"
	ConfigCommand   Command = "CONFIG"
	KeysCommand     Command = "KEYS"
	InfoCommand     Command = "INFO"
	ReplconfCommand Command = "REPLCONF"
	PsyncCommand    Command = "PSYNC"
	WaitCommand     Command = "WAIT"
	IncrCommand     Command = "INCR"
	MultiCommand    Command = "MULTI"
	ExecCommand     Command = "EXEC"
	DiscardCommand  Command = "DISCARD"
	TypeCommand     Command = "TYPE"
	XAddCommand     Command = "XADD"
	XRangeCommand   Command = "XRANGE"
	XReadCommand    Command = "XREAD"
)

// WriteCommands defines commands that modify data
var WriteCommands = []Command{SetCommand, IncrCommand, XAddCommand}

// Handler defines the interface for command handlers
type Handler interface {
	Handle(srv *server.Server, clientConn net.Conn, args []string) error
}

// Registry manages command handlers
type Registry struct {
	handlers map[Command]Handler
}

// NewRegistry creates a new command registry
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[Command]Handler),
	}
}

// Register registers a command handler
func (r *Registry) Register(cmd Command, handler Handler) {
	r.handlers[cmd] = handler
}

// Get retrieves a command handler
func (r *Registry) Get(cmd Command) (Handler, bool) {
	handler, exists := r.handlers[cmd]
	return handler, exists
}

// RegisterAllHandlers registers all available command handlers
func (r *Registry) RegisterAllHandlers() {
	r.Register(PingCommand, &PingHandler{})
	r.Register(EchoCommand, &EchoHandler{})
	r.Register(GetCommand, &GetHandler{})
	r.Register(SetCommand, &SetHandler{})
	r.Register(KeysCommand, &KeysHandler{})
	r.Register(ConfigCommand, &ConfigHandler{})
	r.Register(InfoCommand, &InfoHandler{})
	r.Register(ReplconfCommand, &ReplconfHandler{})
	r.Register(PsyncCommand, &PsyncHandler{})
	r.Register(WaitCommand, &WaitHandler{})
	r.Register(CommandCommand, &CommandHandler{})
	r.Register(IncrCommand, &IncrHandler{})
	r.Register(MultiCommand, &MultiHandler{})
	r.Register(ExecCommand, &ExecHandler{})
	r.Register(DiscardCommand, &DiscardHandler{})
	r.Register(TypeCommand, &TypeHandler{})
	r.Register(XAddCommand, &XAddHandler{})
	r.Register(XRangeCommand, &XRangeHandler{})
	r.Register(XReadCommand, &XReadHandler{})
}
