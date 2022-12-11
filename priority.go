package backpressure

// Priority is the importance of a request, and types in this package prefer higher priority
// requests.
//
// Priorities are non-negative integers and are numbered in decreasing order of priority.
// `Priority(0)` is the highest priority, meaning it is preferred over all other priorities.
// `Priority(1)` is higher priority than all priorities except `Priority(1)`, and so on.
//
// Types in this package generally have overhead per-priority, and so it's recommended to use a
// relatively small number of priorities. Around four works well in practice, for example Critical,
// High, Medium, and Low.
type Priority int
