// Package migrations holds inventory's own schema. Each service migrates its
// own database; the SQL being identical across services today is a coincidence
// of timing, not a shared contract.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
