


import threading

from common.utils import has_won, store_bets, load_bets


class BetsMonitor:
    """Monitor thread safe for handling bets resources"""

    def __init__(self):
        self.lock = threading.Lock()

    def store_bets(self, bets):
        with self.lock:
            store_bets(bets)

    def load_bets(self):
        with self.lock:
            return load_bets()
        
    def has_won(self, bet):
        # has_won util doesn't access to shared resources
        return has_won(bet)