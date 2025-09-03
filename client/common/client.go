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
		linesRead++ // Always increment for every line read

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
		err = c.createClientSocket()
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
		err = c.protocol.SendMessage(c.conn, batchBytes)
		if err != nil {
			log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			continue
		}

		// Receive response
		responseBytes, err := c.protocol.ReceiveMessage(c.conn)
		c.conn.Close()

		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
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
}
