package common

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// Protocol constants
	FIELD_SEPARATOR      = 0x00            // Null byte separator between fields
	BET_SEPARATOR        = 0xFF            // Separator between different bets
	MAX_MESSAGE_SIZE     = 1024 * 1024 * 2 // 2MB
	PAYLOAD_LENGTH_BYTES = 4               // 4 bytes for the length of the message
	RESPONSE_HEADER_SIZE = 2               // 2 bytes for the success flag and separator

	// Message types
	MESSAGE_TYPE_BET_BATCH     = 0x01
	MESSAGE_TYPE_FINISH_NOTIFY = 0x02
	MESSAGE_TYPE_WINNERS_QUERY = 0x03
)

// Protocol handles binary communication protocol
type Protocol struct{}

// NewProtocol creates a new protocol instance
func NewProtocol() *Protocol {
	return &Protocol{}
}

// FinishNotification represents a notification that a client has finished sending all bets
type FinishNotification struct {
	AgencyID string
}

// WinnersQuery represents a query for winners of a specific agency
type WinnersQuery struct {
	AgencyID string
}

// WinnersResponse represents the response with winners for a specific agency
type WinnersResponse struct {
	Success bool
	Message string
	Winners []string // DNIs of winners
	Count   int
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
		totalSize += p.calculateBetSize(&bet) + 1 // +1 for bet separator
	}

	log.Debugf("action: serialize_batch | result: in_progress | totalSize: %v", totalSize)

	// Create buffer
	buffer := make([]byte, totalSize)
	offset := 0

	// Serialize each bet
	for _, bet := range bets {
		p.serializeBetFields(&bet, buffer, &offset)
		buffer[offset] = BET_SEPARATOR // Add bet separator
		offset++
	}

	log.Debugf("action: serialize_batch | result: success | buffer_size: %v", len(buffer))

	return buffer, nil
}

// calculateBetSize calculates the size needed to serialize a single bet
func (p *Protocol) calculateBetSize(bet *BetData) int {
	return len(bet.Agency) + 1 + // agency + separator
		len(bet.Nombre) + 1 + // nombre + separator
		len(bet.Apellido) + 1 + // apellido + separator
		len(bet.DNI) + 1 + // dni + separator
		len(bet.Nacimiento) + 1 + // nacimiento + separator
		len(bet.Numero) // numero (no final separator)
}

// serializeBetFields serializes the fields of a bet into the buffer at the given offset
func (p *Protocol) serializeBetFields(bet *BetData, buffer []byte, offset *int) {
	// Write agency
	copy(buffer[*offset:], []byte(bet.Agency))
	*offset += len(bet.Agency)
	buffer[*offset] = FIELD_SEPARATOR
	*offset++

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
	log.Debugf("action: receive_message | result: in_progress | length: %v", length)

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

// SerializeFinishNotification converts FinishNotification to binary format
func (p *Protocol) SerializeFinishNotification(notification *FinishNotification) ([]byte, error) {
	// Calculate total size: agency_id only (no separator needed)
	totalSize := len(notification.AgencyID)

	buffer := make([]byte, totalSize)
	offset := 0

	// Write agency ID
	copy(buffer[offset:], []byte(notification.AgencyID))

	return buffer, nil
}

// SerializeWinnersQuery converts WinnersQuery to binary format
func (p *Protocol) SerializeWinnersQuery(query *WinnersQuery) ([]byte, error) {
	// Calculate total size: agency_id only (no separator needed)
	totalSize := len(query.AgencyID)

	buffer := make([]byte, totalSize)
	offset := 0

	// Write agency ID
	copy(buffer[offset:], []byte(query.AgencyID))

	return buffer, nil
}

// DeserializeWinnersResponse converts binary data to WinnersResponse
func (p *Protocol) DeserializeWinnersResponse(data []byte) (*WinnersResponse, error) {
	if len(data) < RESPONSE_HEADER_SIZE {
		return nil, errors.New("response data too short")
	}

	resp := &WinnersResponse{}

	// Read success flag
	resp.Success = data[0] == 0x01 // 0x01 for success, 0x00 for failure

	// Skip separator
	if data[1] != FIELD_SEPARATOR {
		return nil, errors.New("invalid response format")
	}

	// Parse the rest of the data (winners list)
	remainingData := data[2:]

	// Parse winners (DNIs separated by FIELD_SEPARATOR)
	winners := []string{}
	currentWinner := ""
	for _, b := range remainingData {
		if b == FIELD_SEPARATOR {
			if currentWinner != "" {
				winners = append(winners, currentWinner)
				currentWinner = ""
			}
		} else {
			currentWinner += string(b)
		}
	}

	// Add last winner if exists
	if currentWinner != "" {
		winners = append(winners, currentWinner)
	}

	resp.Winners = winners
	resp.Count = len(winners)
	resp.Message = fmt.Sprintf("Found %d winners", resp.Count)

	return resp, nil
}

// SendMessageWithType sends a message with type header using the protocol: type + length + data
func (p *Protocol) SendMessageWithType(writer io.Writer, messageType byte, data []byte) error {
	length := uint32(len(data))
	log.Debugf("action: send_message_with_type | result: in_progress | type: %v | length: %v", messageType, length)

	// Send message type (1 byte)
	typeBytes := []byte{messageType}
	err := writeAll(writer, typeBytes)
	if err != nil {
		return err
	}

	// Send length (4 bytes, big endian)
	lengthBytes := make([]byte, PAYLOAD_LENGTH_BYTES)
	binary.BigEndian.PutUint32(lengthBytes, length)
	log.Debugf("action: send_message_with_type | result: in_progress | lengthBytes: %v", lengthBytes)

	err = writeAll(writer, lengthBytes)
	if err != nil {
		return err
	}

	// Send data
	err = writeAll(writer, data)
	return err
}

// ReceiveMessageWithType receives a message with type header using the protocol: type + length + data
func (p *Protocol) ReceiveMessageWithType(reader io.Reader) (byte, []byte, error) {
	// Receive message type (1 byte)
	typeBytes := make([]byte, 1)
	_, err := io.ReadFull(reader, typeBytes)
	if err != nil {
		return 0, nil, err
	}

	messageType := typeBytes[0]
	log.Debugf("action: receive_message_with_type | result: in_progress | type: %v", messageType)

	// Receive length (4 bytes)
	lengthBytes := make([]byte, PAYLOAD_LENGTH_BYTES)
	_, err = io.ReadFull(reader, lengthBytes)
	if err != nil {
		return 0, nil, err
	}

	length := binary.BigEndian.Uint32(lengthBytes)

	// Receive message data
	messageBytes := make([]byte, length)
	_, err = io.ReadFull(reader, messageBytes)
	if err != nil {
		return 0, nil, err
	}

	return messageType, messageBytes, nil
}
