package arctic

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"reflect"
)

var metadataHandshakePrefix []byte = []byte("\x00arctic.metadata.v1\x00")

func (client *Client) Metadata() (metadata map[string]any) {
	metadata = client.metadataSnapshot()
	return
}

func (client *Client) sendMetadataHandshake() (err error) {
	var (
		message     []byte
		conn        net.Conn
		hasDeadline bool = client.config.Timeout > 0
	)

	if message, err = encodeMetadataHandshake(client.metadataSnapshot()); err != nil {
		return
	}

	if err = validateFrameSize(message, metadataBufferSize(client.config.BufferSize)); err != nil {
		return
	}

	if client.config.Protocol == ProtocolUDP {
		err = client.writeUDPDatagram(message)
		return
	}

	conn = client.connection()

	if conn == nil {
		err = ErrClientNotConnected
		return
	}

	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()

	if hasDeadline {
		if err = applyWriteDeadline(conn, client.config.Timeout); err != nil {
			return
		}
	}

	if err = client.writeTCPFrame(conn, message); err != nil {
		client.handleError(err)
	}

	if hasDeadline {
		clearWriteDeadline(conn)
	}

	return
}

func (client *Client) receiveMetadataHandshake(
	reader io.Reader,
	readBuffer *[]byte,
) (firstMessage []byte, hasFirstMessage bool, err error) {
	var (
		metadata map[string]any
		ok       bool
	)

	if firstMessage, err = readFrame(
		reader,
		metadataBufferSize(client.config.BufferSize),
		client.config.UnsafeZeroCopy,
		readBuffer,
	); err != nil {
		return
	}

	if metadata, ok, err = decodeMetadataHandshake(firstMessage); err != nil {
		return
	}

	if ok {
		client.setMetadata(metadata)
		firstMessage = nil
		return
	}

	if err = validateFrameSize(firstMessage, client.config.BufferSize); err != nil {
		return
	}

	hasFirstMessage = true
	return
}

func (client *Client) receiveOptionalMetadataHandshake(reader *bufio.Reader) (err error) {
	var (
		message    []byte
		metadata   map[string]any
		hasFrame   bool
		ok         bool
		readBuffer []byte
	)

	if hasFrame, err = metadataHandshakeFrameAvailable(reader, metadataBufferSize(client.config.BufferSize)); err != nil {
		return
	}

	if !hasFrame {
		return
	}

	if message, err = readFrame(
		reader,
		metadataBufferSize(client.config.BufferSize),
		client.config.UnsafeZeroCopy,
		&readBuffer,
	); err != nil {
		return
	}

	if metadata, ok, err = decodeMetadataHandshake(message); err != nil {
		return
	}

	if ok {
		client.setMetadata(metadata)
	}

	return
}

func metadataHandshakeFrameAvailable(reader *bufio.Reader, bufferSize int) (available bool, err error) {
	var (
		header []byte
		prefix []byte
		length uint32
	)

	if header, err = reader.Peek(frameHeaderSize); err != nil {
		return
	}

	length = binary.BigEndian.Uint32(header)

	if length < uint32(len(metadataHandshakePrefix)) || length > uint32(bufferSize) {
		return
	}

	if prefix, err = reader.Peek(frameHeaderSize + len(metadataHandshakePrefix)); err != nil {
		return
	}

	available = bytes.Equal(prefix[frameHeaderSize:], metadataHandshakePrefix)
	return
}

func encodeMetadataHandshake(metadata map[string]any) (message []byte, err error) {
	var payload []byte

	if metadata == nil {
		metadata = map[string]any{}
	}

	if payload, err = json.Marshal(metadata); err != nil {
		err = fmt.Errorf("%w: %v", ErrMetadataInvalid, err)
		return
	}

	message = make([]byte, len(metadataHandshakePrefix)+len(payload))
	copy(message, metadataHandshakePrefix)
	copy(message[len(metadataHandshakePrefix):], payload)
	return
}

func decodeMetadataHandshake(message []byte) (metadata map[string]any, ok bool, err error) {
	var decoder *json.Decoder
	var payload []byte

	if !bytes.HasPrefix(message, metadataHandshakePrefix) {
		return
	}

	ok = true
	payload = message[len(metadataHandshakePrefix):]

	if len(payload) == 0 {
		metadata = map[string]any{}
		return
	}

	decoder = json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()

	if err = decoder.Decode(&metadata); err != nil {
		err = fmt.Errorf("%w: %v", ErrMetadataInvalid, err)
		return
	}

	if metadata == nil {
		metadata = map[string]any{}
	}

	return
}

func (client *Client) metadataSnapshot() (metadata map[string]any) {
	client.mutex.RLock()
	metadata = cloneMetadata(client.metadata)
	client.mutex.RUnlock()
	return
}

func (client *Client) setMetadata(metadata map[string]any) {
	client.mutex.Lock()
	client.metadata = cloneMetadata(metadata)
	client.mutex.Unlock()
}

func (client *Client) hasMetadata() (exists bool) {
	client.mutex.RLock()
	exists = client.metadata != nil
	client.mutex.RUnlock()
	return
}

func cloneMetadata(source map[string]any) (metadata map[string]any) {
	if source == nil {
		return
	}

	metadata = make(map[string]any, len(source))

	for key, value := range source {
		metadata[key] = cloneMetadataValue(value)
	}

	return
}

func cloneMetadataValue(value any) (cloned any) {
	var (
		encoded []byte
		err     error
	)

	if value == nil {
		return
	}

	switch value.(type) {
	case string, bool, json.Number,
		float32, float64,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		cloned = value
		return
	}

	if !isCompositeMetadataValue(value) {
		cloned = value
		return
	}

	if encoded, err = json.Marshal(value); err != nil {
		cloned = value
		return
	}

	cloned, err = decodeMetadataValue(encoded)

	if err != nil {
		cloned = value
	}

	return
}

func decodeMetadataValue(data []byte) (value any, err error) {
	var decoder *json.Decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err = decoder.Decode(&value)
	return
}

func isCompositeMetadataValue(value any) (composite bool) {
	var kind reflect.Kind = reflect.TypeOf(value).Kind()

	if kind == reflect.Map || kind == reflect.Slice || kind == reflect.Array {
		composite = true
	}

	return
}

func metadataBufferSize(bufferSize int) (size int) {
	size = bufferSize

	if size < defaultBufferSize {
		size = defaultBufferSize
	}

	return
}
