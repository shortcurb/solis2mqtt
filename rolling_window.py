from collections import deque
from time import monotonic
WINDOW_SECONDS = 10 * 60

class RollingWindow:
    def __init__(self, window_seconds=WINDOW_SECONDS):
        self.window_seconds = window_seconds
        self.values = deque()  # stores (timestamp, value)
    def add(self, value):
        now = monotonic()
        self.values.append((now, value))
        self._trim(now)
    def _trim(self, now=None):
        if now is None:
            now = monotonic()
        cutoff = now - self.window_seconds
        while self.values and self.values[0][0] < cutoff:
            self.values.popleft()
    def get_values(self):
        self._trim()
        return list(self.values)
