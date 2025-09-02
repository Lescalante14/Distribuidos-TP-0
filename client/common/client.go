package common

import (
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
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
		protocol: NewProtocol(),
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

// StartClientLoop Send lottery bets to the server until some time threshold is met
func (c *Client) StartClientLoop() {
	// Set up signal handlers
	c.setupSignalHandlers()

	// Send bets in a loop
	for msgID := 1; msgID <= c.config.LoopAmount && !c.endGracefully; msgID++ {
		// Check for shutdown signal before each iteration
		c.handleShutdown()
		if c.endGracefully {
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			break
		}

		// Create the connection to the server
		err := c.createClientSocket()
		if err != nil {
			log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
			continue
		}

		// Prepare bet data for binary protocol
		betDataBinary := &BetDataBinary{
			Nombre:     c.betData.Nombre,
			Apellido:   c.betData.Apellido,
			DNI:        c.betData.DNI,
			Nacimiento: c.betData.Nacimiento,
			Numero:     c.betData.Numero,
		}

		// Serialize bet data to binary format
		betBytes, err := c.protocol.SerializeBet(betDataBinary)
		if err != nil {
			log.Errorf("action: serialize_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
			c.conn.Close()
			continue
		}

		// Send bet data
		err = c.protocol.SendMessage(c.conn, betBytes)
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

		// Check if bet was successful
		if response.Success {
			log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v",
				c.betData.DNI,
				c.betData.Numero,
			)
		} else {
			log.Errorf("action: apuesta_enviada | result: fail | client_id: %v | message: %v", c.config.ID, response.Message)
		}

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

	if c.endGracefully {
		log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
	} else {
		log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
	}
}
