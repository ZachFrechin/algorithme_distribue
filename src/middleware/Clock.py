from threading import Lock

class LogicalClock:
    """
    Gestion de l'horloge logique de Lamport pour un processus.
    """

    def __init__(self):
        self.clock = 0
        self._clock_mutex = Lock()

    def increment(self):
        """Incrémente l'horloge et retourne la nouvelle valeur."""
        with self._clock_mutex:
            self.clock += 1
            return self.clock

    def get_time(self):
        """Retourne la valeur actuelle de l'horloge."""
        with self._clock_mutex:
            return self.clock

    def update_on_receive(self, received_clock):
        """Met à jour l'horloge lors de la réception d'un message."""
        with self._clock_mutex:
            self.clock = max(self.clock, received_clock) + 1
            return self.clock

    def set_time(self, clock_value):
        """Force une valeur d'horloge (utilisé rarement)."""
        with self._clock_mutex:
            self.clock = clock_value