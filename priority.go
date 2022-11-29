package load_mgmt

type Priority int

const (
	Critical Priority = iota
	High
	Medium
	Low
)
