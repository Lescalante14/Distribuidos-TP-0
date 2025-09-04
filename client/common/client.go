package common

import (
	"bufio"
	"fmt"
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

// Client Entity that encapsulates how
type Client struct {
	config        ClientConfig
	conn          net.Conn
	shutdownChan  chan os.Signal
	endGracefully bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:       config,
		shutdownChan: make(chan os.Signal, 1),
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

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	// Set up signal handlers
	c.setupSignalHandlers()

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount && !c.endGracefully; msgID++ {
		// Check for shutdown signal before each iteration
		c.handleShutdown()
		if c.endGracefully {
			log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
			break
		}

		// Create the connection the server in every loop iteration. Send an
		c.createClientSocket()

		// TODO: Modify the send to avoid short-write
		fmt.Fprintf(
			c.conn,
			"[CLIENT %v] Message N°%v\n",
			c.config.ID,
			msgID,
		)
		msg, err := bufio.NewReader(c.conn).ReadString('\n')
		c.conn.Close()

		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
			c.config.ID,
			msg,
		)

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
