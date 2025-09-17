from threading import Lock

class Com:
    def __init__(self, process):
        self.process = process
        self.clock = 0
        self._clock_mutex = Lock()

    def inc_clock(self): 
        with self._clock_mutex:
            self.clock += 1

    def get_clock(self):
        with self._clock_mutex:
            return self.clock

    def _update_clock_on_receive(self, clock):
        with self._clock_mutex:
            self.clock = max(self.clock, clock) + 1

    def _set_clock(self, clock):
        with self._clock_mutex:
            self.clock = clock