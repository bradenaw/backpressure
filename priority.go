package backpressure

type Priority int

const (
	Critical Priority = iota
	High
	Medium
	Low
)

const nPriorities = 4
