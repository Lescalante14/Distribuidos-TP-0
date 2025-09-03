import socket
import logging
import signal

from common.utils import Bet, store_bets

from common.protocol import Protocol, Response


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._end_gracefully = False
        self._protocol = Protocol()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self):
        """
        Lottery Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while not self._end_gracefully:
            print("accepting new connection...")
            client_sock = self.__accept_new_connection()
            if client_sock is None:
                break  # Server is shutting down
            self.__handle_client_connection(client_sock)
        
        logging.info('action: shutdown | result: in_progress')
        self._server_socket.close()
        logging.info('action: shutdown | result: success | resource: server_socket')
    
    def _signal_handler(self, signum, frame):
        """
        Signal handler for graceful shutdown
        """
        logging.info(f'action: signal_received | result: success | signal: {signum}')
        self._end_gracefully = True

    def __handle_client_connection(self, client_sock):
        """
        Handle client connection for lottery bets
        """
        try:
            addr = client_sock.getpeername()
            
            # Receive bet data using protocol
            bet_data_bytes = self._protocol.receive_message(client_sock)
            if bet_data_bytes is None:
                logging.error(f'action: receive_message | result: fail | ip: {addr[0]}')
                return
            
            # Deserialize batch data
            try:
                bet_data_list = self._protocol.deserialize_batch(bet_data_bytes)
            except ValueError as e:
                logging.error(f'action: deserialize_batch | result: fail | ip: {addr[0]} | error: {e}')
                return
            
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | bets_count: {len(bet_data_list)}')
            
            # Process all bets in the batch
            bets_to_store = []
            success = True
            
            for bet_data in bet_data_list:
                try:
                    # Store the bet
                    bet = Bet(1, bet_data.nombre, bet_data.apellido, bet_data.dni, bet_data.nacimiento, bet_data.numero)
                    bets_to_store.append(bet)
                    # MANDATORY LOG FOR TESTING
                    logging.info(f'action: apuesta_almacenada | result: success | dni: {bet_data.dni} | numero: {bet_data.numero}')
                except Exception as e:
                    logging.error(f'action: process_bet | result: fail | dni: {bet_data.dni} | error: {e}')
                    success = False
                    break
            
            # Store all bets if processing was successful
            if success and bets_to_store:
                try:
                    store_bets(bets_to_store)
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets_to_store)}')
                except Exception as e:
                    logging.error(f'action: store_bets | result: fail | error: {e}')
                    success = False
            elif not success:
                logging.error(f'action: apuesta_recibida | result: fail | cantidad: {len(bet_data_list)}')
            
            # Create response
            if success:
                response = Response(success=True, message=f"Batch of {len(bets_to_store)} bets stored successfully")
            else:
                response = Response(success=False, message="Error processing batch")
            
            # Serialize and send response
            response_bytes = self._protocol.serialize_response(response)
            self._protocol.send_message(client_sock, response_bytes)
            
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        try:
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError as e:
            if self._end_gracefully:
                logging.info('action: accept_connections | result: shutdown')
                return None
            else:
                logging.error(f'action: accept_connections | result: fail | error: {e}')
                raise
