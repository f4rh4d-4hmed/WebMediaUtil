//go:build !windows

package main

import "syscall"

// isParentDead checks whether the given PID is still alive.
func isParentDead(ppid int) bool {
	// Unix: Kill(pid, 0) checks existence without sending any signal.
	err := syscall.Kill(ppid, 0)
	return err != nil
}
