


from collections import defaultdict
import logging
import threading

from common.bets_monitor import BetsMonitor


class LotteryMonitor:
    """Monitor thread safe for handling bets resources"""

    def __init__(self, clients_count):
        # Lottery state
        self._clients_count = clients_count
        self.lock = threading.Lock()
        self._finished_agencies = set()
        self._agency_winners = defaultdict(list) 
        self._lottery_completed = False
        self._bets_monitor = BetsMonitor()
        self.winners_cond_var = threading.Condition()

    def add_finished_agency(self, agency_id):
        with self.lock:
            self._finished_agencies.add(agency_id)
            if len(self._finished_agencies) == self._clients_count and not self._lottery_completed:
                self.perform_lottery()
                self.winners_cond_var.notify_all()


    def add_winners(self, agency_id, winners):
        with self.lock:
            self._agency_winners[agency_id].extend(winners)

    def is_lottery_completed(self):
        with self.lock:
            return self._lottery_completed
        
    def perform_lottery(self):
        try:
            with self.lock:
                # Load all bets
                all_bets = list(self._bets_monitor.load_bets())  # Convert generator to list
                
                # Check each bet to see if it won
                for bet in all_bets:
                    if self._bets_monitor.has_won(bet):  # has_won doesn't take winning_number parameter
                        # Extract agency_id from bet
                        agency_id = str(bet.agency)  # Use the agency field from the bet
                        logging.info(f'action: lottery_completed | result: success | agency: {agency_id} dni: {bet.document}')
                        self._agency_winners[agency_id].append(bet.document)  # Use document field for DNI
                
                self._lottery_completed = True
                
                logging.info(f'action: lottery_completed | result: success | total_bets: {len(all_bets)}')
            
        except Exception as e:
            logging.error(f'action: perform_lottery | result: fail | error: {e}')
            self._lottery_completed = False

    def get_winners(self, agency_id):
        while not self._lottery_completed:
            self.winners_cond_var.wait()
        
        with self.lock:
            return self._agency_winners[agency_id]