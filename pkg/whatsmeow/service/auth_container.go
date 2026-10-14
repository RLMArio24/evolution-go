package whatsmeow_service

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// The state is shared by the service's value receivers, not across services.
type authContainerState struct {
	mu        sync.Mutex
	container *sqlstore.Container
}

func (s *authContainerState) get(create func() (*sqlstore.Container, error)) (*sqlstore.Container, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.container != nil {
		return s.container, nil
	}
	container, err := create()
	if err != nil {
		return nil, err
	}
	s.container = container
	return container, nil
}

func newPostgresAuthContainer(ctx context.Context, db *sql.DB, dbLog waLog.Logger) (*sqlstore.Container, error) {
	if db == nil {
		return nil, fmt.Errorf("PostgreSQL auth database is not initialized")
	}
	// Borrow the bounded pool from main; main owns Close, including on failure.
	container := sqlstore.NewWithDB(db, "postgres", dbLog)
	if err := container.Upgrade(ctx); err != nil {
		return nil, fmt.Errorf("failed to upgrade auth database: %w", err)
	}
	return container, nil
}
