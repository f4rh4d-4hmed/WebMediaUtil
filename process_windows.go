//go:build windows

package main

import "syscall"

// isParentDead checks whether the given PID is still alive.
func isParentDead(ppid int) bool {
	// PROCESS_QUERY_INFORMATION = 0x0400
	handle, err := syscall.OpenProcess(0x0400, false, uint32(ppid))
	if err != nil {
		return true
	}
	defer syscall.CloseHandle(handle)
	var exitCode uint32
	if err := syscall.GetExitCodeProcess(handle, &exitCode); err != nil {
		return true
	}
	return exitCode != 259 // 259 = STILL_ACTIVE
}
