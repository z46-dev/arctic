package arctic

import (
	"context"
	"net"

	"github.com/coder/websocket"
)

const arcticWebSocketSubprotocol string = "arctic.v1"

// Opens a WebSocket and adapts it to the framed stream used by Arctic clients.
func dialWebSocket(config ClientConfig) (conn net.Conn, err error) {
	var (
		ctx       context.Context = context.Background()
		cancel    context.CancelFunc
		webSocket *websocket.Conn
	)

	if config.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, config.Timeout)
		defer cancel()
	}

	webSocket, _, err = websocket.Dial(ctx, config.ServerAddress, &websocket.DialOptions{
		Subprotocols: []string{arcticWebSocketSubprotocol},
	})
	if err != nil {
		return
	}

	conn = websocket.NetConn(context.Background(), webSocket, websocket.MessageBinary)
	return
}
