package eventstore

import "errors"

// ErrVersionConflict means another writer appended to this stream after the
// caller folded its state. The caller reloads and retries; it is not a failure
// condition, it is how concurrency is resolved (spec §6.2).
var ErrVersionConflict = errors.New("eventstore: version conflict")
