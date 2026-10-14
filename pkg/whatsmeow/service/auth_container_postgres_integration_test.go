//go:build integration

package whatsmeow_service

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

// This is a fixed, test-only loopback service, never a production/config DSN.
const authPoolTestDSN = "host=127.0.0.1 port=65432 user=evogo_auth_pool_test password=ephemeral-auth-pool-test dbname=evogo_auth_pool_test sslmode=disable connect_timeout=5"

func isolatedAuthPool(t *testing.T, schema string, maxConnections int) *sql.DB {
	t.Helper()
	// Fixed schema names also prevent identifier injection in setup/cleanup SQL.
	if schema != "auth_pool_reuse" && schema != "auth_pool_concurrent" && schema != "auth_pool_retry" {
		t.Fatal("unexpected integration schema")
	}
	admin, err := sql.Open("postgres", authPoolTestDSN)
	if err != nil {
		t.Fatal(err)
	}
	admin.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = admin.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var database, user string
	if err := admin.QueryRowContext(ctx, "SELECT current_database(), current_user").Scan(&database, &user); err != nil {
		t.Fatal(err)
	}
	if database != "evogo_auth_pool_test" || user != "evogo_auth_pool_test" {
		t.Fatal("refusing integration writes outside the isolated test database/role")
	}
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("clean up isolated schema: %v", err)
		}
	})
	db, err := sql.Open("postgres", authPoolTestDSN+" search_path="+schema+" application_name="+schema)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertAuthPoolUsable(t *testing.T, ctx context.Context, db *sql.DB, container *sqlstore.Container) {
	t.Helper()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("borrowed pool is not open: %v", err)
	}
	if _, err := container.GetAllDevices(ctx); err != nil {
		t.Fatalf("upgraded device store is not usable: %v", err)
	}
}

func TestAuthContainerPostgresIntegration(t *testing.T) {
	if os.Getenv("EVOGO_AUTH_POOL_INTEGRATION") != "isolated-postgres" {
		t.Skip("requires explicit opt-in for the isolated PostgreSQL service")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		t.Fatal("isolated PostgreSQL integration is restricted to the Actions runner")
	}

	t.Run("Reuse", func(t *testing.T) {
		db := isolatedAuthPool(t, "auth_pool_reuse", 3)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		state := &authContainerState{}
		calls := 0
		create := func() (*sqlstore.Container, error) {
			calls++
			return newPostgresAuthContainer(ctx, db, nil)
		}
		first, err := state.get(create)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			next, err := state.get(create)
			if err != nil || next != first {
				t.Fatalf("reconnect %d: container=%p, want=%p, error=%v", i, next, first, err)
			}
			assertAuthPoolUsable(t, ctx, db, next)
		}
		if calls != 1 {
			t.Fatalf("factory called %d times, want 1", calls)
		}
	})

	t.Run("ConcurrentInitialization", func(t *testing.T) {
		const clients = 32
		db := isolatedAuthPool(t, "auth_pool_concurrent", 3)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		state := &authContainerState{}
		calls := 0
		create := func() (*sqlstore.Container, error) {
			calls++
			return newPostgresAuthContainer(ctx, db, nil)
		}
		start := make(chan struct{})
		containers := make(chan *sqlstore.Container, clients)
		errorsCh := make(chan error, clients)
		var wg sync.WaitGroup
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				container, err := state.get(create)
				if err == nil {
					_, err = container.GetAllDevices(ctx)
				}
				containers <- container
				errorsCh <- err
			}()
		}
		close(start)
		wg.Wait()
		close(containers)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		for container := range containers {
			if container == nil || container != state.container {
				t.Fatal("concurrent initialization did not share one container")
			}
		}
		if calls != 1 {
			t.Fatalf("factory called %d times, want 1", calls)
		}
		if stats := db.Stats(); stats.MaxOpenConnections != 3 || stats.OpenConnections > 3 {
			t.Fatalf("borrowed pool exceeded its bound: %+v", stats)
		}
		assertAuthPoolUsable(t, ctx, db, state.container)
	})

	t.Run("RetryAfterInitializationFailure", func(t *testing.T) {
		// One persistent pooled connection makes the session restriction deterministic.
		db := isolatedAuthPool(t, "auth_pool_retry", 1)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "SET default_transaction_read_only = on"); err != nil {
			t.Fatal(err)
		}
		state := &authContainerState{}
		calls := 0
		create := func() (*sqlstore.Container, error) {
			calls++
			return newPostgresAuthContainer(ctx, db, nil)
		}
		failed, err := state.get(create)
		var pgErr *pq.Error
		if failed != nil || !errors.As(err, &pgErr) || pgErr.Code != "25006" {
			t.Fatalf("expected PostgreSQL read-only initialization failure (25006), got %v", err)
		}
		if state.container != nil {
			t.Fatal("failed initialization was cached")
		}
		if err := db.PingContext(ctx); err != nil {
			t.Fatalf("failed initialization closed borrowed pool: %v", err)
		}
		if _, err := db.ExecContext(ctx, "SET default_transaction_read_only = off"); err != nil {
			t.Fatal(err)
		}
		recovered, err := state.get(create)
		if err != nil {
			t.Fatalf("retry after restoring writes: %v", err)
		}
		again, err := state.get(create)
		if err != nil || again != recovered || calls != 2 {
			t.Fatalf("reuse after retry: calls=%d, error=%v", calls, err)
		}
		assertAuthPoolUsable(t, ctx, db, recovered)
	})
}
