package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	Timeout        time.Duration
	BatchMaxAmount int
}

// BetData represents a lottery bet
type BetData struct {
	Agency     string
	Nombre     string
	Apellido   string
	DNI        string
	Nacimiento string
	Numero     string
}

const BET_DATA_FIELDS_COUNT = 6

// Client Entity that encapsulates how
type Client struct {
	config        ClientConfig
	conn          net.Conn
	shutdownChan  chan os.Signal
	endGracefully bool
	betData       BetData
	protocol      *Protocol
	csvFilePath   string
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:       config,
		shutdownChan: make(chan os.Signal, 1),
		betData: BetData{
			Agency:     os.Getenv("CLI_ID"),
			Nombre:     os.Getenv("NOMBRE"),
			Apellido:   os.Getenv("APELLIDO"),
			DNI:        os.Getenv("DOCUMENTO"),
			Nacimiento: os.Getenv("NACIMIENTO"),
			Numero:     os.Getenv("NUMERO"),
		},
		protocol:    NewProtocol(),
		csvFilePath: fmt.Sprintf("/data/agency-%s.csv", config.ID),
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	// Set timeout to close if deadlocked
	if c.config.Timeout > 0 {
		conn.SetDeadline(time.Now().Add(c.config.Timeout))
	}
	c.conn = conn
	return nil
}

// setupSignalHandlers sets up signal handlers for graceful shutdown
func (c *Client) setupSignalHandlers() {
	signal.Notify(c.shutdownChan, syscall.SIGTERM, syscall.SIGINT)
}

// handleShutdown handles graceful shutdown
func (c *Client) handleShutdown() {
	select {
	case sig := <-c.shutdownChan:
		log.Infof("action: signal_received | result: success | signal: %v", sig)
		c.endGracefully = true
		// Close connection if it exists
		if c.conn != nil {
			c.conn.Close()
			log.Infof("action: shutdown | result: success | resource: client_connection")
		}
	default:
		// No signal received, continue
	}
}

// readBetsChunk reads a chunk of bets from the CSV file starting from a given position
func (c *Client) readBetsChunk(scanner *bufio.Scanner, chunkSize int) []BetData {
	var bets []BetData
	linesRead := 0

	// Read exactly chunkSize lines or until EOF
	for linesRead < chunkSize && scanner.Scan() {

		line := scanner.Text()

		if line == "" {
			continue
		}

		// Parse CSV line manually since the format is simple
		fields := strings.Split(line, ",")
		if len(fields) != BET_DATA_FIELDS_COUNT-1 { // -1 because the first field is the agency
			log.Warningf("action: parse_csv_line | result: fail | line: %s | reason: invalid field count", line)
			continue
		}

		bet := BetData{
			Agency:     c.config.ID,
			Nombre:     strings.TrimSpace(fields[0]),
			Apellido:   strings.TrimSpace(fields[1]),
			DNI:        strings.TrimSpace(fields[2]),
			Nacimiento: strings.TrimSpace(fields[3]),
			Numero:     strings.TrimSpace(fields[4]),
		}
		bets = append(bets, bet)
		linesRead++
	}

	return bets
}

// StartClientLoop Send lottery bets to the server until some time threshold is met
func (c *Client) StartClientLoop() {
	// Set up signal handlers
	c.setupSignalHandlers()

	// Open CSV file for streaming
	file, err := os.Open(c.csvFilePath)
	if err != nil {
		log.Criticalf("action: open_csv | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	totalBetsProcessed := 0
	batchNum := 1

	log.Infof("action: start_processing | result: success | client_id: %v | batch_size: %v", c.config.ID, c.config.BatchMaxAmount)

	// Create the connection to the server once and keep it open
	log.Infof("action: connect | result: in_progress | client_id: %v", c.config.ID)
	err = c.createClientSocket()
	if err != nil {
		log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	defer c.conn.Close()
	log.Infof("action: connect | result: success | client_id: %v", c.config.ID)

	// Process bets in chunks
	for batchNum <= c.config.LoopAmount && !c.endGracefully {
		// Check for shutdown signal before each iteration
		c.handleShutdown()
		if c.endGracefully {
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			break
		}

		// Read next chunk of bets
		batchBets := c.readBetsChunk(scanner, c.config.BatchMaxAmount)

		// Debug: Log the chunk size and batch number
		log.Infof("action: read_chunk | result: success | client_id: %v | batch_num: %v | chunk_size: %v",
			c.config.ID, batchNum, len(batchBets))

		// If no bets read, we've reached the end of file
		if len(batchBets) == 0 {
			log.Infof("action: eof_reached | result: success | client_id: %v | total_bets_processed: %v", c.config.ID, totalBetsProcessed)
			break
		}

		// Serialize batch of bets
		batchBytes, err := c.protocol.SerializeBatch(batchBets)
		if err != nil {
			log.Errorf("action: serialize_batch | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		// Send batch data
		err = c.protocol.SendMessageWithType(c.conn, MESSAGE_TYPE_BET_BATCH, batchBytes)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		// Receive response
		responseType, responseBytes, err := c.protocol.ReceiveMessageWithType(c.conn)
		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			continue
		}

		// Validate response type (should be the same as the message type we sent)
		if responseType != MESSAGE_TYPE_BET_BATCH {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: unexpected response type %v", c.config.ID, responseType)
			continue
		}

		// Parse response
		response, err := c.protocol.DeserializeResponse(responseBytes)
		if err != nil {
			log.Errorf("action: parse_response | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		// Check if batch was successful
		if response.Success {
			totalBetsProcessed += len(batchBets)
			log.Infof("action: apuesta_enviada | result: success | client_id: %v | batch_size: %v | total_processed: %v",
				c.config.ID,
				len(batchBets),
				totalBetsProcessed,
			)
		} else {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | message: %v", c.config.ID, response.Message)
		}

		batchNum++

		// Wait a time between sending one message and the next one
		// Use a timer that can be interrupted by signals
		timer := time.NewTimer(c.config.LoopPeriod)
		select {
		case <-timer.C:
			// Timer completed, continue
		case <-c.shutdownChan:
			// Signal received during sleep
			log.Infof("action: signal_received | result: success | signal: received during sleep")
			c.endGracefully = true
			timer.Stop()
		}
	}

	// Check for scanner errors after processing is complete
	if err := scanner.Err(); err != nil {
		log.Errorf("action: scanner_error | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}

	if c.endGracefully {
		log.Infof("action: shutdown | result: success | client_id: %v | total_bets_processed: %v", c.config.ID, totalBetsProcessed)
	} else {
		log.Infof("action: loop_finished | result: success | client_id: %v | total_bets_processed: %v", c.config.ID, totalBetsProcessed)
	}

	// Phase 2: Send finish notification and query winners using the same connection
	if !c.endGracefully {
		c.sendFinishNotificationAndQueryWinners()
	}
	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

// sendFinishNotificationAndQueryWinners sends finish notification and queries winners using the same connection
func (c *Client) sendFinishNotificationAndQueryWinners() {
	// Step 1: Send finish notification
	err := c.sendFinishNotification()
	if err != nil {
		log.Errorf("action: finish_notification | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Step 2: Query winners immediately after
	err = c.queryWinners()
	if err != nil {
		log.Errorf("action: query_winners | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
}

// sendFinishNotification sends a notification that this client has finished sending all bets
func (c *Client) sendFinishNotification() error {
	// Create finish notification
	notification := &FinishNotification{
		AgencyID: c.config.ID,
	}

	// Serialize notification
	notificationBytes, err := c.protocol.SerializeFinishNotification(notification)
	if err != nil {
		return err
	}

	// Send notification with type
	err = c.protocol.SendMessageWithType(c.conn, MESSAGE_TYPE_FINISH_NOTIFY, notificationBytes)
	if err != nil {
		return err
	}

	// Receive response
	responseType, responseBytes, err := c.protocol.ReceiveMessageWithType(c.conn)
	if err != nil {
		return err
	}

	// Validate response type (should be the same as the message type we sent)
	if responseType != MESSAGE_TYPE_FINISH_NOTIFY {
		log.Errorf("action: receive_message | result: fail | client_id: %v | error: unexpected response type %v", c.config.ID, responseType)
		return fmt.Errorf("unexpected response type %v", responseType)
	}

	// Parse response
	response, err := c.protocol.DeserializeResponse(responseBytes)
	if err != nil {
		return err
	}

	if response.Success {
		log.Infof("action: finish_notification | result: success | client_id: %v", c.config.ID)
	} else {
		log.Errorf("action: finish_notification | result: fail | client_id: %v | message: %v", c.config.ID, response.Message)
	}

	return nil
}

// queryWinners queries the winners for this agency using the same connection
func (c *Client) queryWinners() error {
	log.Infof("action: query_winners | result: in_progress | client_id: %v", c.config.ID)

	// Create winners query
	query := &WinnersQuery{
		AgencyID: c.config.ID,
	}

	// Serialize query
	queryBytes, err := c.protocol.SerializeWinnersQuery(query)
	if err != nil {
		log.Errorf("action: serialize_query | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Send query with type
	err = c.protocol.SendMessageWithType(c.conn, MESSAGE_TYPE_WINNERS_QUERY, queryBytes)
	if err != nil {
		log.Errorf("action: send_query | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Receive response
	responseType, responseBytes, err := c.protocol.ReceiveMessageWithType(c.conn)
	if err != nil {
		log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	// Validate response type
	if responseType != MESSAGE_TYPE_WINNERS_QUERY {
		log.Errorf("action: receive_response | result: fail | client_id: %v | error: unexpected response type %v", c.config.ID, responseType)
		return fmt.Errorf("unexpected response type %v", responseType)
	}

	// Parse winners response
	winnersResponse, err := c.protocol.DeserializeWinnersResponse(responseBytes)
	if err != nil {
		log.Errorf("action: parse_response | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return err
	}

	if winnersResponse.Success {
		log.Infof("action: consulta_ganadores | result: success | client_id: %v | cant_ganadores: %v", c.config.ID, winnersResponse.Count)
		return nil
	} else {
		log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | message: %v", c.config.ID, winnersResponse.Message)
		return fmt.Errorf("winners query failed: %s", winnersResponse.Message)
	}
}
