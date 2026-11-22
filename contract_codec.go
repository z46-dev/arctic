package arctic

import (
	"bytes"
	"encoding/gob"
)

func encodeContractValue(value any) (encoded []byte, err error) {
	var buffer bytes.Buffer
	if err = gob.NewEncoder(&buffer).Encode(value); err != nil {
		return
	}

	encoded = buffer.Bytes()
	return
}

func decodeContractValue(encoded []byte, value any) (err error) {
	err = gob.NewDecoder(bytes.NewReader(encoded)).Decode(value)
	return
}
