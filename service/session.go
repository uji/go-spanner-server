package service

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

// SessionManager manages sessions and transactions.
type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*SessionInfo
}

// SessionInfo holds session state.
type SessionInfo struct {
	Name         string
	Transactions map[string]*TxInfo
}

// TxInfo holds transaction state.
type TxInfo struct {
	ID       []byte
	ReadOnly bool
}

// NewSessionManager creates a new session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*SessionInfo),
	}
}

// CreateSession creates a new session with the given name.
func (sm *SessionManager) CreateSession(name string) *SessionInfo {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sess := &SessionInfo{
		Name:         name,
		Transactions: make(map[string]*TxInfo),
	}
	sm.sessions[name] = sess
	return sess
}

// GetSession returns a session by name.
func (sm *SessionManager) GetSession(name string) (*SessionInfo, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	s, ok := sm.sessions[name]
	return s, ok
}

// DeleteSession removes a session.
func (sm *SessionManager) DeleteSession(name string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, name)
}

// BeginTransaction starts a new transaction on a session.
func (sm *SessionManager) BeginTransaction(sessionName string, readOnly bool) (*TxInfo, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sess, ok := sm.sessions[sessionName]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionName)
	}

	id := generateTxID()
	tx := &TxInfo{
		ID:       id,
		ReadOnly: readOnly,
	}
	sess.Transactions[hex.EncodeToString(id)] = tx
	return tx, nil
}

func generateTxID() []byte {
	b := make([]byte, 16)
	rand.Read(b)
	return b
}
