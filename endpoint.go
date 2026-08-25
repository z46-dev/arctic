package arctic

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"syscall"
)

var arcticEndpoints = struct {
	sync.Mutex
	inUse map[string]struct{}
}{inUse: map[string]struct{}{}}

func reserveEndpoint(network string, address string) (release func(), err error) {
	var portText string
	if _, portText, err = net.SplitHostPort(address); err != nil {
		return nil, err
	}

	var port int
	if port, err = strconv.Atoi(portText); err != nil {
		if port, err = net.LookupPort(network, portText); err != nil {
			return nil, err
		}
	}

	if port == 0 {
		return func() {}, nil
	}

	var key string = fmt.Sprintf("%s/%d", network, port)
	arcticEndpoints.Lock()
	if _, exists := arcticEndpoints.inUse[key]; exists {
		arcticEndpoints.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrEndpointInUse, key)
	}

	arcticEndpoints.inUse[key] = struct{}{}
	arcticEndpoints.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			arcticEndpoints.Lock()
			delete(arcticEndpoints.inUse, key)
			arcticEndpoints.Unlock()
		})
	}, nil
}

func endpointListenError(network string, address string, err error) (listenErr error) {
	if errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, syscall.Errno(10048)) {
		listenErr = fmt.Errorf("%w: %s/%s: %v", ErrEndpointInUse, network, address, err)
	} else {
		listenErr = err
	}

	return
}
