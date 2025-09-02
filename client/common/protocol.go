package common

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Protocol constants
	ENDIANNESS_MARKER = 0x01 // Little endian marker
	FIELD_SEPARATOR   = 0x00 // Null byte separator
)

// BetDataBinary represents a lottery bet in binary format
type BetDataBinary struct {
	Nombre     string
	Apellido   string
	DNI        string
	Nacimiento string
	Numero     string
}

// Protocol handles binary communication protocol
type Protocol struct{}

// NewProtocol creates a new protocol instance
func NewProtocol() *Protocol {
	return &Protocol{}
}

// SerializeBet converts BetData to binary format
func (p *Protocol) SerializeBet(bet *BetDataBinary) ([]byte, error) {
	log.Debugf("action: serialize_bet | result: in_progress | bet: %v", bet)
	// Calculate total size
	totalSize := 1                       // endianness marker
	totalSize += len(bet.Nombre) + 1     // nombre + separator
	totalSize += len(bet.Apellido) + 1   // apellido + separator
	totalSize += len(bet.DNI) + 1        // dni + separator
	totalSize += len(bet.Nacimiento) + 1 // nacimiento + separator
	totalSize += len(bet.Numero)         // numero (no final separator)
	log.Debugf("action: serialize_bet | result: in_progress | totalSize: %v", totalSize)

	// Create buffer
	buffer := make([]byte, totalSize)
	offset := 0

	// Write endianness marker
	buffer[offset] = ENDIANNESS_MARKER
	offset++

	// Write nombre
	copy(buffer[offset:], []byte(bet.Nombre))
	offset += len(bet.Nombre)
	buffer[offset] = FIELD_SEPARATOR
	offset++

	// Write apellido
	copy(buffer[offset:], []byte(bet.Apellido))
	offset += len(bet.Apellido)
	buffer[offset] = FIELD_SEPARATOR
	offset++

	// Write dni
	copy(buffer[offset:], []byte(bet.DNI))
	offset += len(bet.DNI)
	buffer[offset] = FIELD_SEPARATOR
	offset++

	// Write nacimiento
	copy(buffer[offset:], []byte(bet.Nacimiento))
	offset += len(bet.Nacimiento)
	buffer[offset] = FIELD_SEPARATOR
	offset++

	// Write numero (no final separator)
	copy(buffer[offset:], []byte(bet.Numero))

	log.Debugf("action: serialize_bet | result: success | buffer: %v", buffer)

	return buffer, nil
}

// SendMessage sends a message using the protocol: length + data
func (p *Protocol) SendMessage(writer io.Writer, data []byte) error {
	length := uint32(len(data))
	log.Debugf("action: send_message | result: in_progress | length: %v", length)

	// Send length (4 bytes, big endian)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, length)
	log.Debugf("action: send_message | result: in_progress | lengthBytes: %v", lengthBytes)

	//TODO: check if this ensure that the message is sent completely?
	_, err := writer.Write(lengthBytes)
	if err != nil {
		return err
	}

	// Send data
	_, err = writer.Write(data)
	return err
}

// ReceiveMessage receives a message using the protocol: length + data
func (p *Protocol) ReceiveMessage(reader io.Reader) ([]byte, error) {
	// Receive length (4 bytes)
	lengthBytes := make([]byte, 4)
	log.Debugf("action: receive_message | result: in_progress | lengthBytes: %v", lengthBytes)
	_, err := io.ReadFull(reader, lengthBytes)
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	// Receive message data
	messageBytes := make([]byte, length)
	_, err = io.ReadFull(reader, messageBytes)
	if err != nil {
		return nil, err
	}

	return messageBytes, nil
}

// Response represents server response
type Response struct {
	Success bool
	Message string
}

// DeserializeResponse converts binary data to Response
func (p *Protocol) DeserializeResponse(data []byte) (*Response, error) {
	if len(data) < 3 {
		return nil, errors.New("response data too short")
	}

	// Check endianness marker
	if data[0] != ENDIANNESS_MARKER {
		return nil, errors.New("invalid endianness marker")
	}

	resp := &Response{}

	// Read success flag
	resp.Success = data[1] == 0x01

	// Skip separator
	if data[2] != FIELD_SEPARATOR {
		return nil, errors.New("invalid response format")
	}

	// Read message (rest of the data)
	resp.Message = string(data[3:])

	return resp, nil
}
