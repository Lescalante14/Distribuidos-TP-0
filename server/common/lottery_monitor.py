


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
        self.winners_cond_var = threading.Condition(self.lock)

    def add_finished_agency(self, agency_id):
        should_perform_lottery = False
        with self.lock:
            if agency_id in self._finished_agencies: # avoid run twice the lottery
                return
            self._finished_agencies.add(agency_id)
            should_perform_lottery = (
                len(self._finished_agencies) == self._clients_count
                and not self._lottery_completed
            )
        if should_perform_lottery:
            self.perform_lottery()


    def add_winners(self, agency_id, winners):
        with self.lock:
            self._agency_winners[agency_id].extend(winners)

    def is_lottery_completed(self):
        with self.lock:
            return self._lottery_completed
        
    def perform_lottery(self):
        try:
            # 1) heavy work outside the lock
            all_bets = self._bets_monitor.load_bets()
            local_winners = defaultdict(list)

            for bet in all_bets:
                if self._bets_monitor.has_won(bet):
                    agency_id = str(bet.agency)
                    local_winners[agency_id].append(bet.document)
                    logging.info(
                        f'action: lottery_candidate | result: success | agency: {agency_id} dni: {bet.document}'
                    )

            # 2) write state and notify under the lock
            with self.lock:
                for agency_id, docs in local_winners.items():
                    self._agency_winners[agency_id].extend(docs)
                self._lottery_completed = True
                self.winners_cond_var.notify_all()

            logging.info(
                f'action: lottery_completed | result: success | total_bets: {len(all_bets)}'
            )

        except Exception as e:
            logging.error(f'action: perform_lottery | result: fail | error: {e}')
            with self.lock:
                self._lottery_completed = False

    def get_winners(self, agency_id):
        with self.lock:
            while not self._lottery_completed:
                self.winners_cond_var.wait()
            return self._agency_winners[agency_id]