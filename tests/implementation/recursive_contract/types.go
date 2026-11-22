package recursive_contract

//go:generate go tool github.com/z46-dev/arctic/cmd/arcticgen

type (
	Request struct{}

	Message struct {
		Value string
	}

	Response struct {
		Value string
	}

	DeepestContractDefinition interface {
		__ArcticContract__() struct{}

		Leaf(Message) Response
	}

	DeepContractDefinition interface {
		__ArcticContract__() struct {
			OpenLeaf DeepestContractDefinition `arctic:"streaming"`
		}

		OpenLeaf(Request) Response
	}

	NestedContractDefinition interface {
		__ArcticContract__() struct {
			OpenDeep DeepContractDefinition `arctic:"streaming"`
		}

		OpenDeep(Request) Response
	}

	StreamingContractDefinition interface {
		__ArcticContract__() struct {
			OpenNested NestedContractDefinition `arctic:"streaming"`
		}

		OpenNested(Request) Response
	}

	RootContractDefinition interface {
		__ArcticContract__() struct {
			OpenRoot StreamingContractDefinition `arctic:"streaming"`
		}

		OpenRoot(Request) Response
	}
)
