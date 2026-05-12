//go:build darwin

package main

import "syscall"

// isParentDead checks whether the given PID is still alive on macOS.
func isParentDead(ppid int) bool {
	// Darwin: Kill(pid, 0) checks existence without sending any signal.
	err := syscall.Kill(ppid, 0)
	return err != nil
}
