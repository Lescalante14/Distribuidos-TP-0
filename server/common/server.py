import socket
import logging
import signal
import threading
from collections import defaultdict

from common.utils import Bet, store_bets, load_bets, has_won
from common.protocol import Protocol, Response, FinishNotification, WinnersQuery, WinnersResponse, MESSAGE_TYPE_BET_BATCH, MESSAGE_TYPE_FINISH_NOTIFY, MESSAGE_TYPE_WINNERS_QUERY
from common.bets_monitor import BetsMonitor
from common.lottery_monitor import LotteryMonitor


class Server:
    def __init__(self, port, listen_backlog, clients_count):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._end_gracefully = False
        self._protocol = Protocol()
        self._clients_count = clients_count

        # Initialize monitors for thread safety
        self._bets_monitor = BetsMonitor()
        self._lottery_monitor = LotteryMonitor(clients_count)
        
        # Client thread management
        self._client_threads = {}  # client_id -> thread
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self):
        """
        Lottery Server loop with multithreading

        Server that accepts new connections and creates a thread for each client.
        Each client thread handles all communication with that client.
        """
        logging.info('action: server_start | result: success | clients_count: {}'.format(self._clients_count))

        while not self._end_gracefully:
            try:
                print("accepting new connection...")
                client_sock = self.__accept_new_connection()
                if client_sock is None:
                    break  # Server is shutting down
                
                # Create a new thread for this client
                client_thread = threading.Thread(target=self.__handle_client_connection, args=(client_sock,))
                client_thread.start()
                
                # Store thread reference (we'll use socket as key for now)
                self._client_threads[client_sock] = client_thread
                
            except Exception as e:
                logging.error(f'action: accept_connection | result: fail | error: {e}')
                if not self._end_gracefully:
                    continue
                else:
                    break
        
        # Wait for all client threads to finish
        self.__wait_for_client_threads()
        
        logging.info('action: shutdown | result: in_progress')
        # Server socket might already be closed by signal handler
        try:
            self._server_socket.close()
            logging.info('action: shutdown | result: success | resource: server_socket')
        except:
            logging.info('action: shutdown | result: success | resource: server_socket_already_closed')
    
    def __wait_for_client_threads(self):
        """Wait for all client threads to finish"""
        threads_to_join = list(self._client_threads.values())
        
        # Join all threads
        for thread in threads_to_join:
            thread.join()
        
        # Clean up thread references
        self._client_threads.clear()
        
        logging.info('action: client_threads_joined | result: success')
    
    def _signal_handler(self, signum, frame):
        """
        Signal handler for graceful shutdown
        """
        logging.info(f'action: signal_received | result: success | signal: {signum}')
        self._end_gracefully = True
        # Close server socket to unblock accept() call
        try:
            self._server_socket.close()
        except:
            pass  # Socket might already be closed

    def __handle_client_connection(self, client_sock):
        """
        Handle client connection for lottery bets in a dedicated thread
        """
        client_id = None
        try:
            addr = client_sock.getpeername()
            client_id = f"{addr[0]}:{addr[1]}"
            logging.info(f'action: client_connected | result: success | client_id: {client_id}')
            
            # Keep connection open and handle multiple messages
            while not self._end_gracefully:
                try:
                    # Always receive message with type (mandatory)
                    message_type, message_data = self._protocol.receive_message_with_type(client_sock)
                    if message_type is None or message_data is None:
                        logging.info(f'action: client_disconnected | result: success | client_id: {client_id}')
                        break
                    
                    # Handle different message types
                    if message_type == MESSAGE_TYPE_BET_BATCH:
                        self.__handle_bet_batch(client_sock, message_data, client_id)
                    elif message_type == MESSAGE_TYPE_FINISH_NOTIFY:
                        self.__handle_finish_notification(client_sock, message_data, client_id)
                    elif message_type == MESSAGE_TYPE_WINNERS_QUERY:
                        self.__handle_winners_query(client_sock, message_data, client_id)
                    else:
                        logging.error(f'action: unknown_message_type | result: fail | client_id: {client_id} | type: {message_type}')
                        response = Response(success=False, message="Unknown message type")
                        response_bytes = self._protocol.serialize_response(response)
                        self._protocol.send_message_with_type(client_sock, message_type, response_bytes)
                
                except OSError as e:
                    logging.info(f'action: client_disconnected | result: success | client_id: {client_id} | error: {e}')
                    break
                except Exception as e:
                    logging.error(f'action: handle_message | result: fail | client_id: {client_id} | error: {e}')
                    break
                    
        except Exception as e:
            logging.error(f'action: client_connection | result: fail | client_id: {client_id} | error: {e}')
        finally:
            # Clean up client socket
            try:
                client_sock.close()
            except:
                pass
            
            logging.info(f'action: client_cleanup | result: success | client_id: {client_id}')

    def __handle_bet_batch(self, client_sock, message_data, client_id):
        """Handle bet batch message"""
        try:
            # Deserialize batch data
            bet_data_list = self._protocol.deserialize_batch(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_batch | result: fail | client_id: {client_id} | error: {e}')
            return
        
        logging.info(f'action: receive_message | result: success | client_id: {client_id} | bets_count: {len(bet_data_list)}')
        
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
        
        # Store all bets if processing was successful using thread-safe monitor
        if success and bets_to_store:
            try:
                self._bets_monitor.store_bets(bets_to_store)
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

    def __handle_finish_notification(self, client_sock, message_data, client_id):
        """Handle finish notification message"""
        try:
            # Deserialize finish notification
            notification = self._protocol.deserialize_finish_notification(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_finish_notification | result: fail | client_id: {client_id} | error: {e}')
            return
        
        logging.info(f'action: finish_notification | result: success | agency: {notification.agency_id}')
        
        # Add agency to finished set using thread-safe monitor
        self._lottery_monitor.add_finished_agency(notification.agency_id)
        
        # Send response with type
        response = Response(success=True, message="Finish notification received")
        response_bytes = self._protocol.serialize_response(response)
        self._protocol.send_message_with_type(client_sock, MESSAGE_TYPE_FINISH_NOTIFY, response_bytes)

    def __handle_winners_query(self, client_sock, message_data, client_id):
        """Handle winners query message"""
        try:
            # Deserialize winners query
            query = self._protocol.deserialize_winners_query(message_data)
        except ValueError as e:
            logging.error(f'action: deserialize_winners_query | result: fail | client_id: {client_id} | error: {e}')
            return
        
        logging.info(f'action: winners_query | result: success | agency: {query.agency_id}')
        
        # Get winners using thread-safe monitor (this will wait if lottery not completed)
        winners = self._lottery_monitor.get_winners(query.agency_id)
        
        # Create response
        response = WinnersResponse(
            success=True, 
            message=f"Found {len(winners)} winners", 
            winners=winners, 
            count=len(winners)
        )
        
        # Send response with type
        response_bytes = self._protocol.serialize_winners_response(response)
        self._protocol.send_message_with_type(client_sock, MESSAGE_TYPE_WINNERS_QUERY, response_bytes)

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
