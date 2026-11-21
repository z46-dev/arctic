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

	ContractDefinition interface {
		__ArcticContract__() struct {
			Join struct{} `arctic:"direction=client-server,one-at-once"`
		}

		Join(InitialRequest) GenericResponse
		Math(MathRequest) MathResponse
		Done(struct{}) GenericResponse
	}
)

const (
	MathOperandAddition MathOperand = iota
	MathOperandSubtraction
	MathOperandMultiplication
	MathOperandDivision
)
