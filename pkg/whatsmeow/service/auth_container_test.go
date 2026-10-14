package whatsmeow_service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/store/sqlstore/upgrades"
)

func expectCurrentAuthSchema(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SELECT EXISTS").WithArgs("whatsmeow_version", "compat").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("SELECT version, compat FROM whatsmeow_version LIMIT 1").
		WillReturnRows(sqlmock.NewRows([]string{"version", "compat"}).AddRow(len(upgrades.Table), len(upgrades.Table)))
}

func TestAuthContainerReconnectReuse(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectCurrentAuthSchema(mock)
	state := &authContainerState{}
	// Copying a service receiver retains this same state pointer.
	copiedState := state
	calls := 0
	create := func() (*sqlstore.Container, error) {
		calls++
		return newPostgresAuthContainer(context.Background(), db, nil)
	}
	first, err := state.get(create)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		next, err := copiedState.get(create)
		if err != nil || next != first {
			t.Fatalf("reconnect %d: container=%p, want=%p, error=%v", i, next, first, err)
		}
	}
	if calls != 1 {
		t.Fatalf("factory called %d times, want 1", calls)
	}
	// The service must leave the borrowed pool open for its owner.
	mock.ExpectPing()
	if err := db.Ping(); err != nil {
		t.Fatalf("borrowed authDB was closed: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthContainerConcurrentInitialization(t *testing.T) {
	const clients = 32
	state := &authContainerState{}
	want := &sqlstore.Container{}
	start := make(chan struct{})
	results := make(chan *sqlstore.Container, clients)
	errorsCh := make(chan error, clients)
	calls := 0
	create := func() (*sqlstore.Container, error) {
		calls++
		return want, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			container, err := state.get(create)
			results <- container
			errorsCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	for got := range results {
		if got != want {
			t.Fatalf("container=%p, want=%p", got, want)
		}
	}
	if calls != 1 {
		t.Fatalf("factory called %d times, want 1", calls)
	}
}

func TestAuthContainerFailureInitializationRetry(t *testing.T) {
	if container, err := newPostgresAuthContainer(context.Background(), nil, nil); err == nil || container != nil {
		t.Fatal("missing borrowed authDB must fail without opening a fallback pool")
	}
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	upgradeError := errors.New("temporary database failure")
	mock.ExpectQuery("SELECT EXISTS").WithArgs("whatsmeow_version", "compat").WillReturnError(upgradeError)
	state := &authContainerState{}
	calls := 0
	create := func() (*sqlstore.Container, error) {
		calls++
		return newPostgresAuthContainer(context.Background(), db, nil)
	}
	if container, err := state.get(create); container != nil || !errors.Is(err, upgradeError) {
		t.Fatalf("failed initialization: container=%p, error=%v", container, err)
	}
	if state.container != nil {
		t.Fatal("failed initialization was cached")
	}
	mock.ExpectPing()
	if err := db.Ping(); err != nil {
		t.Fatalf("failed initialization closed borrowed authDB: %v", err)
	}
	expectCurrentAuthSchema(mock)
	recovered, err := state.get(create)
	if err != nil || recovered == nil {
		t.Fatalf("retry: container=%p, error=%v", recovered, err)
	}
	again, err := state.get(create)
	if err != nil || again != recovered || calls != 2 {
		t.Fatalf("reuse after retry: container=%p, want=%p, calls=%d, error=%v", again, recovered, calls, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
