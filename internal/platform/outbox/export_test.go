package outbox

// ClaimSQL exposes the poller's claim query to the package's external tests, so
// the query-plan test explains the statement the poller actually runs rather than
// a copy of it that can drift out of step.
var ClaimSQL = claimSQL
