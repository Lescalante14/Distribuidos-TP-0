import socket
import logging
import signal
from collections import defaultdict

from common.utils import Bet, store_bets, load_bets, has_won
from common.protocol import Protocol, Response, FinishNotification, WinnersQuery, WinnersResponse, MESSAGE_TYPE_BET_BATCH, MESSAGE_TYPE_FINISH_NOTIFY, MESSAGE_TYPE_WINNERS_QUERY


class Server:
    def __init__(self, port, listen_backlog, clients_count):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._end_gracefully = False
        self._protocol = Protocol()
        self._clients_count = clients_count

        # Lottery state
        self._finished_agencies = set()
        self._lottery_completed = False
        self._agency_winners = defaultdict(list)  # agency_id -> list of winning DNIs
        
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
            
            # Always receive message with type (mandatory)
            message_type, message_data = self._protocol.receive_message_with_type(client_sock)
            if message_type is None or message_data is None:
                logging.error(f'action: receive_message | result: fail | ip: {addr[0]} | error: failed to receive message with type')
                return
            
            # Handle different message types
            if message_type == MESSAGE_TYPE_BET_BATCH:
                self.__handle_bet_batch(client_sock, message_data, addr)
            elif message_type == MESSAGE_TYPE_FINISH_NOTIFY:
                self.__handle_finish_notification(client_sock, message_data, addr)
            elif message_type == MESSAGE_TYPE_WINNERS_QUERY:
                self.__handle_winners_query(client_sock, message_data, addr)
            else:
                logging.error(f'action: unknown_message_type | result: fail | ip: {addr[0]} | type: {message_type}')
                response = Response(success=False, message="Unknown message type")
                response_bytes = self._protocol.serialize_response(response)
                self._protocol.send_message_with_type(client_sock, message_type, response_bytes)
            
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __handle_bet_batch(self, client_sock, message_data, addr):
        """Handle bet batch message"""
        try:
            # Deserialize batch data
            bet_data_list = self._protocol.deserialize_batch(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_batch | result: fail | ip: {addr[0]} | error: {e}')
            return
        
        logging.info(f'action: receive_message | result: success | ip: {addr[0]} | bets_count: {len(bet_data_list)}')
        
        # Process all bets in the batch
        bets_to_store = []
        success = True
        
        for bet in bet_data_list:
            try:
                # Store the bet
                bets_to_store.append(bet)
                # MANDATORY LOG FOR TESTING ej 5
                # logging.info(f'action: apuesta_almacenada | result: success | dni: {bet_data.dni} | numero: {bet_data.numero}')
            except Exception as e:
                logging.error(f'action: process_bet | result: fail | dni: {bet.document} | error: {e}')
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
        
        # Serialize and send response with type
        response_bytes = self._protocol.serialize_response(response)
        self._protocol.send_message_with_type(client_sock, MESSAGE_TYPE_BET_BATCH, response_bytes)

    def __handle_finish_notification(self, client_sock, message_data, addr):
        """Handle finish notification message"""
        try:
            # Deserialize finish notification
            notification = self._protocol.deserialize_finish_notification(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_finish_notification | result: fail | ip: {addr[0]} | error: {e}')
            return
        
        logging.info(f'action: finish_notification | result: success | agency: {notification.agency_id}')
        
        # Add agency to finished set
        self._finished_agencies.add(notification.agency_id)
        
        # Check if all agencies have finished
        if len(self._finished_agencies) == self._clients_count and not self._lottery_completed:
            self.__perform_lottery()
        
        # Send response with type
        response = Response(success=True, message="Finish notification received")
        response_bytes = self._protocol.serialize_response(response)
        self._protocol.send_message_with_type(client_sock, MESSAGE_TYPE_FINISH_NOTIFY, response_bytes)

    def __handle_winners_query(self, client_sock, message_data, addr):
        """Handle winners query message"""
        try:
            # Deserialize winners query
            query = self._protocol.deserialize_winners_query(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_winners_query | result: fail | ip: {addr[0]} | error: {e}')
            return
        
        logging.info(f'action: winners_query | result: success | agency: {query.agency_id} lottery completed: {self._lottery_completed}')
        
        # Check if lottery has been completed
        if not self._lottery_completed:
            response = WinnersResponse(success=False, message="Lottery not yet completed", winners=[], count=0)
        else:
            # Get winners for this agency
            logging.info(f'action: winners_query | result: success | agency: {query.agency_id} winners: {self._agency_winners}')
            winners = self._agency_winners.get(query.agency_id, [])
            response = WinnersResponse(success=True, message=f"Found {len(winners)} winners", winners=winners, count=len(winners))
        
        # Send response with type
        response_bytes = self._protocol.serialize_winners_response(response)
        self._protocol.send_message_with_type(client_sock, MESSAGE_TYPE_WINNERS_QUERY, response_bytes)
        client_sock.close() #TODO: Remove this

    def __perform_lottery(self):
        """Perform the lottery and determine winners"""
        logging.info("action: sorteo | result: success")
        
        try:
            # Load all bets
            all_bets = list(load_bets())  # Convert generator to list
            
            # Check each bet to see if it won
            for bet in all_bets:
                if has_won(bet):  # has_won doesn't take winning_number parameter
                    # Extract agency_id from bet
                    agency_id = str(bet.agency)  # Use the agency field from the bet
                    logging.info(f'action: lottery_completed | result: success | agency: {agency_id} dni: {bet.document}')
                    self._agency_winners[agency_id].append(bet.document)  # Use document field for DNI
            
            self._lottery_completed = True
            
            logging.info(f'action: lottery_completed | result: success | total_bets: {len(all_bets)}')
            
        except Exception as e:
            logging.error(f'action: perform_lottery | result: fail | error: {e}')
            self._lottery_completed = False

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
