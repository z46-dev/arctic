package arctic

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
)

func validateFrameSize(message []byte, bufferSize int) (err error) {
	if len(message) > bufferSize {
		err = errMessageTooLarge(len(message), bufferSize)
		return
	}

	return
}

func isExpectedCloseError(err error) (expected bool) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		expected = true
	}

	if strings.Contains(err.Error(), "use of closed network connection") {
		expected = true
	}

	return
}

func errMessageTooLarge(size int, bufferSize int) (err error) {
	err = fmt.Errorf("%w: %d > %d", ErrMessageTooLarge, size, bufferSize)
	return
}
