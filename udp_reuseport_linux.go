//go:build linux

package arctic

import "syscall"

const linuxSOReusePort int = 0xf

func udpReusePortAvailable() (available bool) {
	available = true
	return
}

func setUDPReusePort(network string, address string, conn syscall.RawConn) (err error) {
	var controlErr error

	err = conn.Control(func(fd uintptr) {
		if controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); controlErr != nil {
			return
		}

		controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, linuxSOReusePort, 1)
	})

	if err != nil {
		return
	}

	err = controlErr
	return
}
