// Package srtest starts an Apicurio Registry for integration tests.
//
// The URL it returns already includes the Confluent-compatibility path prefix,
// because that is the single most common way to waste an hour here: Apicurio
// serves the Confluent-shaped API under /apis/ccompat/v7, not at the root, and a
// client pointed at the bare host gets 404 on every call (spec §8.5).
package srtest

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	Image = "apicurio/apicurio-registry:3.3.1"
	port  = "8080/tcp"
	// ccompatPath is the prefix Apicurio serves the Confluent-shaped API under. It
	// is part of the URL, not an optional extra — see the package comment.
	ccompatPath = "/apis/ccompat/v7"
)

type Registry struct{ url string }

// URL is the ccompat base URL, ready to pass to sr.URLs.
func (r *Registry) URL() string { return r.url }

var shared *Registry

// Shared returns the registry Start brought up, skipping the test in -short mode.
func Shared(t *testing.T) *Registry {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping container test in -short mode")
	}
	if shared == nil {
		t.Fatal("srtest: no registry running — this package needs a TestMain that calls srtest.Start")
	}
	return shared
}

// Start brings up the package's registry and returns a stop function. Call it
// from TestMain, exactly as with pgtest.Start (spec §12.4).
//
// One registry per package means tests share subjects, so a test that needs an
// *unregistered* subject asks about a topic no other test registers rather than
// starting a second registry.
func Start() (stop func(), err error) {
	if !flag.Parsed() {
		flag.Parse() // testing.Short() panics before flags are parsed
	}
	if testing.Short() {
		return func() {}, nil
	}
	ctx := context.Background()

	ctr, err := runRegistry(ctx)
	if err != nil {
		return nil, err
	}
	addr, err := hostAddress(ctx, ctr)
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, err
	}

	shared = &Registry{url: "http://" + addr + ccompatPath}
	return func() {
		shared = nil
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintf(os.Stderr, "srtest: terminate: %v\n", err)
		}
	}, nil
}

// runRegistry starts the container and waits for it to answer on its own API.
func runRegistry(ctx context.Context) (*testcontainers.DockerContainer, error) {
	ctr, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts(port),
		testcontainers.WithEnv(map[string]string{
			// Apicurio 3.x has no "mem" storage kind; sql over in-memory H2 is
			// its ephemeral store. Set explicitly so a registry started for a
			// test cannot quietly pick up persistent storage.
			"APICURIO_STORAGE_KIND":     "sql",
			"APICURIO_STORAGE_SQL_KIND": "h2",
		}),
		testcontainers.WithWaitStrategyAndDeadline(2*time.Minute,
			wait.ForHTTP("/apis/registry/v3/system/info").WithPort(port)),
	)
	if err != nil {
		return nil, fmt.Errorf("srtest: start %s: %w", Image, err)
	}
	return ctr, nil
}

// hostAddress is the host:port this test process can reach the registry on.
func hostAddress(ctx context.Context, ctr *testcontainers.DockerContainer) (string, error) {
	host, err := ctr.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("srtest: host: %w", err)
	}
	mapped, err := ctr.MappedPort(ctx, port)
	if err != nil {
		return "", fmt.Errorf("srtest: mapped port: %w", err)
	}
	return host + ":" + mapped.Port(), nil
}
