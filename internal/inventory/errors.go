package inventory

import "errors"

// ErrUnknownEvent means a stored seat event has a type this binary does not
// know. Only inventory appends to a seat stream, so this is a binary older than
// its own data, not a message from a stranger — folding past it would silently
// produce the wrong state, so it fails instead.
var ErrUnknownEvent = errors.New("inventory: unknown seat event")

// ErrUnknownCommand means a message arrived on inventory.commands that is not a
// command. It is permanent: redelivery cannot make it a command, so retrying
// would only repeat the same failure — the consumer dead-letters it instead.
var ErrUnknownCommand = errors.New("inventory: unknown command")
