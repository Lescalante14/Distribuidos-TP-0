package common

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Protocol constants
	FIELD_SEPARATOR      = 0x00            // Null byte separator
	MAX_MESSAGE_SIZE     = 1024 * 1024 * 2 // 2MB
	PAYLOAD_LENGTH_BYTES = 4               // 4 bytes for the length of the message
	RESPONSE_HEADER_SIZE = 2               // 2 bytes for the success flag and separator
)

// Protocol handles binary communication protocol
type Protocol struct{}

// NewProtocol creates a new protocol instance
func NewProtocol() *Protocol {
	return &Protocol{}
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// SerializeBet converts BetData to binary format
func (p *Protocol) SerializeBet(bet *BetData) ([]byte, error) {
	log.Debugf("action: serialize_bet | result: in_progress | bet: %v", bet)

	// Calculate total size
	totalSize := p.calculateBetSize(bet)
	log.Debugf("action: serialize_bet | result: in_progress | totalSize: %v", totalSize)

	// Create buffer
	buffer := make([]byte, totalSize)
	offset := 0

	// Serialize the bet
	p.serializeBetFields(bet, buffer, &offset)

	log.Debugf("action: serialize_bet | result: success | buffer: %v", buffer)

	return buffer, nil
}

// SerializeBatch converts a slice of BetData to binary format
func (p *Protocol) SerializeBatch(bets []BetData) ([]byte, error) {
	log.Debugf("action: serialize_batch | result: in_progress | bets_count: %v", len(bets))

	// Calculate total size for all bets
	totalSize := 0
	for _, bet := range bets {
		totalSize += p.calculateBetSize(&bet)
	}

	log.Debugf("action: serialize_batch | result: in_progress | totalSize: %v", totalSize)

	// Create buffer
	buffer := make([]byte, totalSize)
	offset := 0

	// Serialize each bet
	for _, bet := range bets {
		p.serializeBetFields(&bet, buffer, &offset)
	}

	log.Debugf("action: serialize_batch | result: success | buffer_size: %v", len(buffer))

	return buffer, nil
}

// calculateBetSize calculates the size needed to serialize a single bet
func (p *Protocol) calculateBetSize(bet *BetData) int {
	return len(bet.Nombre) + 1 + // nombre + separator
		len(bet.Apellido) + 1 + // apellido + separator
		len(bet.DNI) + 1 + // dni + separator
		len(bet.Nacimiento) + 1 + // nacimiento + separator
		len(bet.Numero) // numero (no final separator)
}

// serializeBetFields serializes the fields of a bet into the buffer at the given offset
func (p *Protocol) serializeBetFields(bet *BetData, buffer []byte, offset *int) {
	// Write nombre
	copy(buffer[*offset:], []byte(bet.Nombre))
	*offset += len(bet.Nombre)
	buffer[*offset] = FIELD_SEPARATOR
	*offset++

	// Write apellido
	copy(buffer[*offset:], []byte(bet.Apellido))
	*offset += len(bet.Apellido)
	buffer[*offset] = FIELD_SEPARATOR
	*offset++

	// Write dni
	copy(buffer[*offset:], []byte(bet.DNI))
	*offset += len(bet.DNI)
	buffer[*offset] = FIELD_SEPARATOR
	*offset++

	// Write nacimiento
	copy(buffer[*offset:], []byte(bet.Nacimiento))
	*offset += len(bet.Nacimiento)
	buffer[*offset] = FIELD_SEPARATOR
	*offset++

	// Write numero (no final separator)
	copy(buffer[*offset:], []byte(bet.Numero))
	*offset += len(bet.Numero)
}

// SendMessage sends a message using the protocol: length + data
func (p *Protocol) SendMessage(writer io.Writer, data []byte) error {
	length := uint32(len(data))
	log.Debugf("action: send_message | result: in_progress | length: %v", length)

	// Send length (4 bytes, big endian)
	lengthBytes := make([]byte, PAYLOAD_LENGTH_BYTES)
	binary.BigEndian.PutUint32(lengthBytes, length)
	log.Debugf("action: send_message | result: in_progress | lengthBytes: %v", lengthBytes)

	err := writeAll(writer, lengthBytes)
	if err != nil {
		return err
	}

	// Send data
	err = writeAll(writer, data)
	return err
}

// ReceiveMessage receives a message using the protocol: length + data
func (p *Protocol) ReceiveMessage(reader io.Reader) ([]byte, error) {
	// Receive length (4 bytes)
	lengthBytes := make([]byte, PAYLOAD_LENGTH_BYTES)
	log.Debugf("action: receive_message | result: in_progress | lengthBytes: %v", lengthBytes)
	_, err := io.ReadFull(reader, lengthBytes) //readfull is guaranteed to read the entire message
	if err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	// Receive message data
	messageBytes := make([]byte, length)
	_, err = io.ReadFull(reader, messageBytes) //readfull is guaranteed to read the entire message
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
	if len(data) < RESPONSE_HEADER_SIZE {
		return nil, errors.New("response data too short")
	}

	resp := &Response{}

	// Read success flag
	resp.Success = data[0] == 0x01 // 0x01 for success, 0x00 for failure

	// Skip separator
	if data[1] != FIELD_SEPARATOR {
		return nil, errors.New("invalid response format")
	}

	// Read message (rest of the data)
	resp.Message = string(data[2:])

	return resp, nil
}
