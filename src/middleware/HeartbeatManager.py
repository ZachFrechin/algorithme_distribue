"""Gestion des heartbeats pour détection de pannes et réorganisation.

Ce module envoie périodiquement des heartbeats, détecte les processus
échoués, déclenche des réélections et renumérote les processus actifs
si nécessaire.
"""
from time import time, sleep
from threading import Thread, Lock
from Message import HeartbeatMessage, ProcessCountUpdateMessage, IdReassignmentMessage
from pyeventbus3.pyeventbus3 import PyBus

class HeartbeatManager:
    """
    Gestionnaire des heartbeats pour détecter les processus morts
    et corriger la numérotation automatiquement.
    """

    def __init__(self, process, clock, logger):
        self.process = process
        self.clock = clock
        self.logger = logger

        # Configuration heartbeat
        self.heartbeat_interval = 2.0  # Envoyer un heartbeat toutes les 2 secondes
        self.failure_timeout = 5.0     # Considérer mort après 5 secondes sans heartbeat

        # Suivi des processus vivants
        self.active_processes = set()
        self.last_heartbeat = {}  # process_id -> timestamp
        self.heartbeat_lock = Lock()

        # Thread de surveillance
        self.monitoring_thread = None
        self.running = False

    def start(self):
        """Démarre le système de heartbeat."""
        if self.process.id_assigned:
            self.running = True

            # Démarrer le thread de surveillance
            self.monitoring_thread = Thread(target=self._monitoring_loop, daemon=True)
            self.monitoring_thread.start()

            self.logger.log("Heartbeat system started")

    def stop(self):
        """Arrête le système de heartbeat."""
        self.running = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=1.0)

    def _monitoring_loop(self):
        """Boucle principale de surveillance des heartbeats."""
        while self.running and self.process.alive:
            try:
                # Envoyer notre heartbeat
                self._send_heartbeat()

                # Vérifier les processus morts (seulement le coordinateur)
                if self.process.is_coordinator:
                    self._check_failed_processes()
                else:
                    # Les non-coordinateurs vérifient si le coordinateur est mort
                    self._check_coordinator_alive()

                sleep(self.heartbeat_interval)
            except Exception as e:
                self.logger.log(f"Heartbeat monitoring error: {e}")
                break

    def _send_heartbeat(self):
        """Envoie un heartbeat à tous les autres processus."""
        if not self.process.id_assigned:
            return

        # Mettre à jour notre propre timestamp
        current_time = time()
        with self.heartbeat_lock:
            self.active_processes.add(self.process.myId)
            self.last_heartbeat[self.process.myId] = current_time

        self.clock.increment()
        heartbeat = HeartbeatMessage.new_heartbeat_message(
            self.clock.get_time(), self.process.myId
        )
        heartbeat.broadcast(PyBus.Instance())

    def _check_failed_processes(self):
        """Vérifie quels processus ont échoué (coordinateur uniquement)."""
        current_time = time()
        failed_processes = []

        with self.heartbeat_lock:
            for process_id in list(self.active_processes):
                # Ne pas vérifier notre propre processus
                if process_id == self.process.myId:
                    continue

                last_seen = self.last_heartbeat.get(process_id, 0)
                if last_seen > 0 and current_time - last_seen > self.failure_timeout:
                    self.logger.log(f"Process P{process_id} last seen {current_time - last_seen:.1f}s ago - marking as failed", coordinator_only=True)
                    failed_processes.append(process_id)
                    self.active_processes.discard(process_id)
                    if process_id in self.last_heartbeat:
                        del self.last_heartbeat[process_id]

        # Traiter les échecs détectés
        for failed_id in failed_processes:
            self._handle_process_failure(failed_id)

    def _check_coordinator_alive(self):
        """Vérifie si le coordinateur est toujours vivant (processus non-coordinateurs)."""
        current_time = time()
        coordinator_id = 0

        with self.heartbeat_lock:
            last_seen = self.last_heartbeat.get(coordinator_id, 0)
            if last_seen > 0 and current_time - last_seen > self.failure_timeout:
                # Coordinateur détecté comme mort
                self.logger.log("Coordinator failure detected, starting new election")
                self._start_election_after_coordinator_failure()

    def _start_election_after_coordinator_failure(self):
        """Lance une nouvelle élection après la mort du coordinateur."""
        # Arrêter le heartbeat temporairement
        self.running = False

        # Réinitialiser les données de processus
        self.process.is_coordinator = False
        self.process.id_assigned = False
        self.process.myId = None

        # Lancer une nouvelle élection via l'ElectionManager
        if hasattr(self.process, 'com') and self.process.com and self.process.com.election_manager:
            self.process.com.election_manager.election_in_progress = False
            self.process.com.election_manager.received_election_numbers.clear()
            self.process.com.election_manager.received_pongs.clear()
            self.process.com.election_manager.request_dynamic_id()

    def _handle_process_failure(self, failed_id):
        """Gère l'échec d'un processus."""
        self.logger.log(f"Process P{failed_id} detected as failed", coordinator_only=True)

        # Si c'est le coordinateur qui est mort, déclencher une réélection
        if failed_id == 0:
            self.logger.log("Coordinator has failed, triggering new election", coordinator_only=True)
            self._start_election_after_coordinator_failure()
        else:
            # Processus normal mort, recalculer la numérotation
            self._renumber_active_processes()

    def _renumber_active_processes(self):
        """Renumérote les processus actifs pour combler les trous."""
        if not self.process.is_coordinator:
            return

        # Créer une nouvelle numérotation compacte
        sorted_active = sorted(self.active_processes)
        old_to_new_mapping = {}

        # Le coordinateur garde toujours l'ID 0
        for i, old_id in enumerate(sorted_active):
            old_to_new_mapping[old_id] = i

        # Si notre mapping change, broadcaster les nouveaux IDs
        changes_made = False
        for old_id, new_id in old_to_new_mapping.items():
            if old_id != new_id:
                changes_made = True
                break

        if changes_made:
            self.logger.log("Renumbering processes after failure", coordinator_only=True)

            # Envoyer les messages de réassignation pour chaque changement d'ID
            for old_id, new_id in old_to_new_mapping.items():
                if old_id != new_id:
                    self.logger.log(f"Reassigning P{old_id} to P{new_id}", coordinator_only=True)
                    self.clock.increment()
                    reassignment = IdReassignmentMessage.new_id_reassignment_message(
                        old_id, new_id, self.clock.get_time(), self.process.myId
                    )
                    reassignment.broadcast(PyBus.Instance())

            # Broadcaster le nouveau compte de processus
            new_count = len(self.active_processes)
            self.clock.increment()
            count_update = ProcessCountUpdateMessage.new_process_count_update_message(
                new_count, self.clock.get_time(), self.process.myId
            )
            count_update.broadcast(PyBus.Instance())

            # Mettre à jour notre propre compte et ID si nécessaire
            if self.process.myId in old_to_new_mapping:
                old_my_id = self.process.myId
                new_my_id = old_to_new_mapping[old_my_id]
                if old_my_id != new_my_id:
                    self.logger.log(f"Coordinator reassigning itself from P{old_my_id} to P{new_my_id}", coordinator_only=True)
                    self.process.myId = new_my_id

            self.process.com.nbProcess = new_count
            self.logger.log(f"Updated process count to {new_count} after failure", coordinator_only=True)

    def handle_heartbeat(self, event):
        """Traite la réception d'un heartbeat."""
        if event.source == self.process.myId:
            return  # Ignorer notre propre heartbeat

        current_time = time()

        with self.heartbeat_lock:
            # Enregistrer ce processus comme actif
            self.active_processes.add(event.source)
            self.last_heartbeat[event.source] = current_time

    def get_active_processes(self):
        """Retourne la liste des processus actuellement actifs."""
        with self.heartbeat_lock:
            return sorted(list(self.active_processes))

    def is_process_alive(self, process_id):
        """Vérifie si un processus est considéré comme vivant."""
        with self.heartbeat_lock:
            return process_id in self.active_processes