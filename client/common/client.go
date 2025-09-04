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
	BatchMaxAmount int
}

// BetData represents a lottery bet
type BetData struct {
	Nombre     string
	Apellido   string
	DNI        string
	Nacimiento string
	Numero     string
}

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
	c.conn = conn
	return nil
}

// setupSignalHandlers sets up signal handlers for graceful shutdown
func (c *Client) setupSignalHandlers() {
	signal.Notify(c.shutdownChan, syscall.SIGTERM)
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
		if len(fields) != 5 {
			log.Warningf("action: parse_csv_line | result: fail | line: %s | reason: invalid field count", line)
			continue
		}

		bet := BetData{
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

		// Create the connection to the server
		log.Infof("action: connect | result: in_progress | client_id: %v", c.config.ID)
		err = c.createClientSocket()
		log.Infof("action: connect | result: success | client_id: %v", c.config.ID)
		if err != nil {
			log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		// Serialize batch of bets
		batchBytes, err := c.protocol.SerializeBatch(batchBets)
		if err != nil {
			log.Errorf("action: serialize_batch | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			continue
		}

		// Send batch data
		err = c.protocol.SendMessageWithType(c.conn, MESSAGE_TYPE_BET_BATCH, batchBytes)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			continue
		}

		// Receive response
		responseType, responseBytes, err := c.protocol.ReceiveMessageWithType(c.conn)
		c.conn.Close()

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

	// Phase 2: Send finish notification and query winners
	if !c.endGracefully {
		c.sendFinishNotificationAndQueryWinners()
	}
	log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
}

// sendFinishNotificationAndQueryWinners sends finish notification and queries winners
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
	// Create connection
	err := c.createClientSocket()
	if err != nil {
		return err
	}
	defer c.conn.Close()

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

// queryWinners queries the winners for this agency using polling
func (c *Client) queryWinners() error {
	maxRetries := 10
	retryDelay := time.Second * 2

	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Infof("action: query_winners | result: in_progress | client_id: %v | attempt: %v/%v", c.config.ID, attempt, maxRetries)

		// Create connection
		err := c.createClientSocket()
		if err != nil {
			log.Errorf("action: connect | result: fail | client_id: %v | attempt: %v | error: %v", c.config.ID, attempt, err)
			time.Sleep(retryDelay)
			continue
		}

		// Create winners query
		query := &WinnersQuery{
			AgencyID: c.config.ID,
		}

		// Serialize query
		queryBytes, err := c.protocol.SerializeWinnersQuery(query)
		if err != nil {
			c.conn.Close()
			log.Errorf("action: serialize_query | result: fail | client_id: %v | attempt: %v | error: %v", c.config.ID, attempt, err)
			time.Sleep(retryDelay)
			continue
		}

		// Send query with type
		err = c.protocol.SendMessageWithType(c.conn, MESSAGE_TYPE_WINNERS_QUERY, queryBytes)
		if err != nil {
			c.conn.Close()
			log.Errorf("action: send_query | result: fail | client_id: %v | attempt: %v | error: %v", c.config.ID, attempt, err)
			time.Sleep(retryDelay)
			continue
		}

		// Receive response
		responseType, responseBytes, err := c.protocol.ReceiveMessageWithType(c.conn)
		c.conn.Close()

		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | attempt: %v | error: %v", c.config.ID, attempt, err)
			time.Sleep(retryDelay)
			continue
		}

		// Validate response type
		if responseType != MESSAGE_TYPE_WINNERS_QUERY {
			log.Errorf("action: receive_response | result: fail | client_id: %v | attempt: %v | error: unexpected response type %v", c.config.ID, attempt, responseType)
			time.Sleep(retryDelay)
			continue
		}

		// Parse winners response
		winnersResponse, err := c.protocol.DeserializeWinnersResponse(responseBytes)
		if err != nil {
			log.Errorf("action: parse_response | result: fail | client_id: %v | attempt: %v | error: %v", c.config.ID, attempt, err)
			time.Sleep(retryDelay)
			continue
		}

		if winnersResponse.Success {
			log.Infof("action: consulta_ganadores | result: success | client_id: %v | cant_ganadores: %v", c.config.ID, winnersResponse.Count)
			return nil // Success, exit polling loop
		} else {
			log.Infof("action: consulta_ganadores | result: waiting | client_id: %v | attempt: %v | message: %v", c.config.ID, attempt, winnersResponse.Message)
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
		}
	}

	log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: max retries exceeded", c.config.ID)
	return fmt.Errorf("max retries exceeded for winners query")
}
