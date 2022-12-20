package backpressure

// Priority is the importance of a request. Types in this package prefer higher priority requests.
//
// Priorities are non-negative integers and are numbered in decreasing order of priority.
// `Priority(0)` is the highest priority, meaning it is preferred over all other priorities.
// `Priority(1)` is higher priority than all priorities except `Priority(0)`, and so on.
//
// Types in this package generally have overhead per-priority and function best when priorities are
// quite coarse, and so it's recommended to use a relatively small number of priorities. Around four
// works well in practice, for example:
//
// Critical - A user is waiting and critical user flows are functional, if severely degraded, if
// only Critical requests are served.
//
// High - A user is waiting, and this request is important to the overall experience. The default
// for anything that a user might see.
//
// Medium - No user is directly waiting, or the request is noncritical to the experience.
// Examples: type-ahead search suggestions, asynchronous work.
//
// Low - This is a request that is content to consume only leftover capacity, and may wait until
// off-peak when there is more capacity available. Examples: daily batch jobs, continuous
// verification, one-offs, backfills.
type Priority int
