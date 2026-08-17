// Package pgtest starts one Postgres container per test package and hands out
// independent databases inside it.
//
// One container per package, started in TestMain, is spec §12.4: container
// startup dominates test time, and isolation comes from separate databases named
// after the test rather than from fresh containers. Separate databases also
// preserve the property that no transaction can span two services.
//
// There is intentionally no exported function that starts a container for a
// single test. A container per test is the mistake this API exists to prevent.
package pgtest

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const Image = "postgres:18.6"

// PG is the package's shared Postgres.
type PG struct {
	baseDSN string
	mu      sync.Mutex
	created map[string]bool
}

var shared *PG

// Start brings up the package's Postgres container and returns a stop function.
// Call it from TestMain:
//
//	func TestMain(m *testing.M) {
//		stop, err := pgtest.Start()
//		if err != nil {
//			fmt.Fprintln(os.Stderr, err)
//			os.Exit(1)
//		}
//		code := m.Run()
//		stop()
//		os.Exit(code)
//	}
//
// In -short mode it starts nothing and returns a no-op stop, so the fast suite
// never touches Docker.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("sagaflow"),
		postgres.WithPassword("sagaflow"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start %s: %w", Image, err)
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("pgtest: connection string: %w", err)
	}
	shared = &PG{baseDSN: dsn, created: map[string]bool{}}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "pgtest: terminate: %v\n", err)
		}
	}, nil
}

// Shared returns the container Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *PG {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("pgtest: no container running — this package needs a TestMain that calls pgtest.Start; see the pgtest package doc")
	}
	return shared
}

// DSN creates dbName on first request and returns a DSN pointing at it. Name the
// database after the test so tests in one package cannot collide.
func (p *PG) DSN(t *testing.T, dbName string) string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.created[dbName] {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, p.baseDSN)
		if err != nil {
			t.Fatalf("connect admin: %v", err)
		}
		defer conn.Close(ctx)
		// CREATE DATABASE takes no parameters, so the name has to be
		// interpolated. pgx.Identifier does the quoting and escaping; fmt's %q
		// only looks right because Go and SQL happen to share the double quote.
		stmt := "CREATE DATABASE " + pgx.Identifier{dbName}.Sanitize()
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("create database %s: %v", dbName, err)
		}
		p.created[dbName] = true
	}
	dsn, err := replaceDBName(p.baseDSN, dbName)
	if err != nil {
		t.Fatalf("build dsn for %s: %v", dbName, err)
	}
	return dsn
}

// replaceDBName points a DSN at a different database, preserving everything else
// including the query string.
//
// Parsed rather than sliced at the last '/': a password or host containing a
// slash would send an index-based rewrite to the wrong place, and this helper's
// output is what every integration test connects through.
func replaceDBName(dsn, dbName string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	u.Path = "/" + dbName
	return u.String(), nil
}
