# Native WebSocket example

This starts an Arctic TCP server on `127.0.0.1:8080`, attaches a WebSocket sidecar on `127.0.0.1:8081`, and connects to the sidecar with a native Go client using the regular Arctic client API.

```bash
go run ./examples/websocket_native
```
