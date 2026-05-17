//go:build !linux

package arctic

import "syscall"

func udpReusePortAvailable() (available bool) {
	return
}

func setUDPReusePort(network string, address string, conn syscall.RawConn) (err error) {
	return
}
