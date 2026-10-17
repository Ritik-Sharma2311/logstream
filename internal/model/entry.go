package model

import "time"

// Entry is a single structured log line.
type Entry struct {
	Timestamp time.Time `json:"ts"`
	Level     string    `json:"level"`
	Service   string    `json:"service"`
	Message   string    `json:"message"`
}
