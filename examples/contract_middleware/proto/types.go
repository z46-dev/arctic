package proto

//go:generate go tool github.com/z46-dev/arctic/cmd/arcticgen

type (
	PingRequest struct {
		Message string
	}

	PingResponse struct {
		Message string
	}

	ContractDefinition interface {
		__ArcticContract__() struct{}
		Ping(PingRequest) PingResponse
	}
)
