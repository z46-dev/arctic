# Browser WebSocket example

Build the Go client as WebAssembly from the repository root:

PowerShell:

```powershell
$env:GOOS = "js"; $env:GOARCH = "wasm"; go build -o examples/websocket_browser/client.wasm ./examples/websocket_browser; Remove-Item Env:GOOS, Env:GOARCH
```

Bash:

```bash
GOOS=js GOARCH=wasm go build -o examples/websocket_browser/client.wasm ./examples/websocket_browser; unset GOOS; unset GOARCH
```

Then start the native Arctic and web servers:

```bash
go run ./examples/websocket_browser
```

Open [http://localhost:8080](http://localhost:8080) in your browser. The page loads `client.wasm`, connects to the WebSocket sidecar, sends a message, and displays the server response.

The generated `client.wasm` file is ignored by Git.
