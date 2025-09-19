"""Thread représentant un processus distribué simulé.

Chaque instance gère:
- l'attribution dynamique d'ID via `ElectionManager`,
- le passage de token pour section critique,
- les barrières de synchronisation,
- un mécanisme de heartbeat pour détecter les pannes et se réorganiser.
"""
from re import S
from threading import Lock, Thread
from time import sleep, time
import time as time_module
from Message import BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage, RegistrationMessage, RequestMaxIdMessage, ResponseMaxIdMessage, IdRequestMessage, IdAssignmentMessage, HeartbeatMessage, IdReassignmentMessage
from pyeventbus3.pyeventbus3 import *
from middleware.Com import Com
from Token import Token


class Process(Thread):
    """Processus logique exécuté dans un thread.

    Args:
        name: Nom lisible du processus (pour les logs).
        npProcess: Nombre total de processus prévu au démarrage.

    Composants:
        - `Com`: middleware (horloge, logger, élection, sync, mailbox, heartbeat)
        - `myId`: ID logique (attribué dynamiquement), `temp_id` avant attribution
        - `token`: token de l'anneau pour l'accès à la section critique
    """

    def __init__(self, name, npProcess):
        Thread.__init__(self)

        self.npProcess = npProcess
        self.myProcessName = name
        self.name = "MainThread-" + name
        self.clock = 0
        self.token = None
        self.SC = 0
        self.sync_ready_count = 0
        self.sync_barrier = False

        self.temp_id = int(time_module.time() * 1000000) % 1000000
        self.myId = None 
        self.is_coordinator = False
        self.next_id_to_assign = 0
        self.id_assigned = False
        self.pending_id_requests = []

        self.active_processes = set()
        self.last_heartbeat = {}
        self.heartbeat_interval = 2.0
        self.failure_timeout = 5.0

        self.com = Com(self)
        self.com.nbProcess = self.npProcess

        self.alive = True
        self.start()

    def run(self):
        """Boucle principale du processus.

        - Demande un ID dynamique.
        - Initialise le ring de token si coordinateur.
        - Déclenche un exemple de section critique.
        - Peut simuler une panne pour tester la reprise.
        """
        # Attribution d'ID dynamique pour processus arrivant à tout moment
        self.com.request_dynamic_id()

        loop = 0
        while self.alive:
            # Logger seulement les loops importants
            self.com.log(f"Loop: {loop}")
            sleep(1)

            # Initialiser le token ring dès que possible
            if self.myId == 0 and loop == 1 and not hasattr(self, 'token_initialized'):
                initial_token = Token("SC_TOKEN")
                next_process = self.next_node()
                self.com.log(f"Token ring initialized, sending to P{next_process}", coordinator_only=True)
                self.com.send_token(initial_token)
                self.token_initialized = True

            # === EXEMPLE SIMPLE DE SECTION CRITIQUE À LA LOOP 3 ===
            if loop == 3 and self.myId == 1:
                self.com.logger.log_section("CRITICAL SECTION EXAMPLE")
                self.com.request_SC()  # Bloque jusqu'à obtention du token
                self.com.log("[CRITICAL SECTION] Accessing shared resource")
                sleep(1)  # Simulation de travail en section critique
                self.com.log("[CRITICAL SECTION] Work completed")
                self.com.release_SC()  # Libère le token

            # Simuler une panne pour tester la réorganisation
            if loop == 8 and self.myId == 2:
                self.com.log("Simulating process failure")
                if hasattr(self, 'com') and self.com:
                    self.com.stop_heartbeat()
                self.alive = False
                return

            loop += 1
        if hasattr(self, 'com'):
            self.com.log("Process stopped")

    def next_node(self):
        """Retourne l'ID du prochain processus actif dans l'anneau."""
        if hasattr(self, 'com') and self.com and self.com.heartbeat_manager:
            # Utiliser les processus actifs pour le token ring
            active_processes = sorted(self.com.heartbeat_manager.get_active_processes())
            try:
                current_index = active_processes.index(self.myId)
                next_index = (current_index + 1) % len(active_processes)
                return active_processes[next_index]
            except ValueError:
                # Fallback si notre ID n'est pas dans la liste
                return (self.myId + 1) % self.com.nbProcess
        else:
            return (self.myId + 1) % self.npProcess

    def stop(self):
        """Demande l'arrêt du thread et stoppe le heartbeat."""
        print(self.name + " stopped")
        self.alive = False
        if hasattr(self, 'com') and self.com:
            self.com.stop_heartbeat()

    def waitStopped(self):
        """Bloque jusqu'à la terminaison du thread."""
        self.join()
