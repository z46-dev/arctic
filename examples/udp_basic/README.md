Run `go run examples/udp_basic/main.go`.

If you wish the server to run on a different address, provide the argument `-addr <address>`.

Ex: `go run examples/udp_basic/main.go -addr "127.0.0.1:8081"`

Raw UDP has no remote close signal. In this example, `OnClose` runs when the local client closes or when the server shuts down its virtual UDP clients.
