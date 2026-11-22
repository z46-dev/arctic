package proto

//go:generate go tool github.com/z46-dev/arctic/cmd/arcticgen

type (
	MathOperand uint8

	InitialRequest struct {
		Message string
	}

	GenericResponse struct {
		OK      bool
		Message string
	}

	MathRequest struct {
		FirstOperator, SecondOperator float64
		Operand                       MathOperand
	}

	MathResponse struct {
		Valid        bool
		ErrorMessage string
		Result       float64
	}

	DataStreamingRequest struct {
		NumExpectedChunks int
	}

	DataStreamingBody struct {
		Chunk []byte
	}

	StreamingContractDefinition interface {
		__ArcticContract__() struct{}
		Data(DataStreamingBody) DataStreamingBody
	}

	ContractDefinition interface {
		__ArcticContract__() struct {
			Join          struct{}                    `arctic:"direction=client-server,one-at-once"`
			DataStreaming StreamingContractDefinition `arctic:"streaming"`
		}

		Join(InitialRequest) GenericResponse
		Math(MathRequest) MathResponse
		Done(struct{}) GenericResponse
		DataStreaming(DataStreamingRequest) GenericResponse
	}
)

const (
	MathOperandAddition MathOperand = iota
	MathOperandSubtraction
	MathOperandMultiplication
	MathOperandDivision
)
