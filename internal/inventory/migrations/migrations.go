// Package migrations is an embed.FS of the SQL files that describe
// inventory's own database schema, applied by tern. Each service migrates
// its own database; the SQL being identical across services today is a
// coincidence of timing, not a shared contract.
//
// The migrations are embedded into the binary rather than shipped alongside
// it as separate files, so the schema a given binary expects travels with
// that binary and cannot be deployed apart from the code that needs it — an
// operator cannot start a build against an older or newer schema than the
// one it was compiled to run against.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
