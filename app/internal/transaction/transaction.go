package transaction

import (
	"net"
	"sync"
)

type QueuedCommand struct {
	Command string
	Args    []string
}

type Transaction struct {
	Commands      []QueuedCommand
	InTransaction bool
	mutex         sync.RWMutex
}

type Manager struct {
	transactions map[net.Conn]*Transaction
	mutex        sync.RWMutex
}

func NewManager() *Manager {
	return &Manager{
		transactions: make(map[net.Conn]*Transaction),
	}
}

func (m *Manager) StartTransaction(conn net.Conn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.transactions[conn] = &Transaction{
		Commands:      make([]QueuedCommand, 0),
		InTransaction: true,
	}
}

func (m *Manager) IsInTransaction(conn net.Conn) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	transaction, exists := m.transactions[conn]
	return exists && transaction.InTransaction
}

func (m *Manager) QueueCommand(conn net.Conn, command string, args []string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if transaction, exists := m.transactions[conn]; exists {
		transaction.mutex.Lock()
		transaction.Commands = append(transaction.Commands, QueuedCommand{
			Command: command,
			Args:    args,
		})
		transaction.mutex.Unlock()
	}
}

func (m *Manager) GetQueuedCommands(conn net.Conn) []QueuedCommand {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if transaction, exists := m.transactions[conn]; exists {
		transaction.mutex.RLock()
		defer transaction.mutex.RUnlock()

		// Return a copy to avoid race conditions
		commands := make([]QueuedCommand, len(transaction.Commands))
		copy(commands, transaction.Commands)
		return commands
	}
	return nil
}

func (m *Manager) EndTransaction(conn net.Conn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.transactions, conn)
}

func (m *Manager) DiscardTransaction(conn net.Conn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.transactions, conn)
}

func (m *Manager) CleanupConnection(conn net.Conn) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.transactions, conn)
}
