"""Gestionnaire d'élections et d'attribution d'ID pour les processus.

Implémente une élection simple basée sur un tirage aléatoire et gère
la demande/assignation d'identifiants lorsqu'un coordinateur existe.
"""
from time import sleep
import random
from Message import BroadcastMessage, IdRequestMessage, IdAssignmentMessage, ProcessCountUpdateMessage
from pyeventbus3.pyeventbus3 import PyBus

class ElectionManager:
    """
    Gestionnaire des élections de leader et attribution d'IDs.
    Système simple : plus grand nombre aléatoire gagne.
    """

    def __init__(self, process, clock, logger):
        self.process = process
        self.clock = clock
        self.logger = logger
        self.my_election_number = None
        self.received_election_numbers = {}  # temp_id -> election_number
        self.election_in_progress = False
        self.received_pongs = []  # Liste des réponses PONG_COORDINATOR reçues

    def request_dynamic_id(self):
        """
        Attribution d'ID par élection simple.
        Si pas de coordinateur, tout le monde tire un nombre aléatoire.
        Le plus grand gagne.
        """
        # Phase 1: Vérifier s'il y a déjà un leader
        if self._check_existing_leader():
            return  # Leader trouvé, ID reçu

        # Phase 2: Lancer une élection simple
        self._start_simple_election()

    def _check_existing_leader(self):
        """Vérifie s'il y a déjà un leader existant."""
        # Ping pour chercher un coordinateur existant
        self.clock.increment()
        ping_msg = BroadcastMessage.new_broadcast_message("PING_COORDINATOR", self.clock.get_time(), self.process.temp_id)
        ping_msg.broadcast(PyBus.Instance())

        sleep(1.0)  # Délai plus long pour laisser le temps aux coordinateurs de répondre

        # Vérifier les réponses PONG reçues
        if self.received_pongs:
            # Coordinateur trouvé silencieusement
            self._request_id_from_existing_leader()
            return True

        # Vérifier s'il y a une élection en cours
        if self.received_election_numbers:
            # Il y a une élection en cours, participer !
            # Participer silencieusement
            self._join_existing_election()
            return True

        return False

    def receive_election_number(self, temp_id, election_number):
        """Recevoir un numéro d'élection directement."""
        self.received_election_numbers[temp_id] = election_number

    def receive_pong_coordinator(self, coordinator_id):
        """Recevoir une réponse PONG_COORDINATOR directement."""
        self.received_pongs.append(coordinator_id)

    def _join_existing_election(self):
        """Participer à une élection déjà en cours."""
        if self.election_in_progress:
            return

        self.election_in_progress = True

        # Tirer notre nombre aléatoire pour participer
        self.my_election_number = random.randint(1, 1000000)

        # L'annoncer
        election_data = f"ELECTION_NUMBER:{self.process.temp_id}:{self.my_election_number}"
        self.clock.increment()
        election_msg = BroadcastMessage.new_broadcast_message(election_data, self.clock.get_time(), self.process.temp_id)
        election_msg.broadcast(PyBus.Instance())

        # Participation silencieuse

        # Attendre que l'élection se termine (délai plus long pour laisser les autres finir)
        sleep(3.0)

        # Déterminer le gagnant
        self._determine_election_winner()

    def _wait_for_existing_election(self):
        """Attendre qu'une élection en cours se termine."""
        self.logger.log("Waiting for existing election to complete")
        sleep(4.0)  # Attendre que l'élection se termine complètement

        # Après l'attente, chercher le coordinateur qui a dû être élu
        if self._check_existing_leader():
            return

        # Si toujours pas de coordinateur, quelque chose s'est mal passé, relancer
        self.logger.log("No coordinator found after waiting, starting new election")
        self._start_simple_election()

    def _start_simple_election(self):
        """Lance une élection simple avec nombre aléatoire."""
        if self.election_in_progress:
            return

        # Vérifier une dernière fois s'il n'y a pas déjà une élection en cours
        if self.received_election_numbers:
            # Rejoindre silencieusement
            self._join_existing_election()
            return

        self.election_in_progress = True

        # Tirer un nombre aléatoire
        self.my_election_number = random.randint(1, 1000000)

        # L'annoncer
        election_data = f"ELECTION_NUMBER:{self.process.temp_id}:{self.my_election_number}"
        self.clock.increment()
        election_msg = BroadcastMessage.new_broadcast_message(election_data, self.clock.get_time(), self.process.temp_id)
        election_msg.broadcast(PyBus.Instance())

        # Tirage silencieux

        # Attendre les autres nombres (délai réduit)
        sleep(1.5)

        # Déterminer le gagnant
        self._determine_election_winner()

    def _determine_election_winner(self):
        """Détermine qui a gagné l'élection."""
        # Récupérer tous les nombres reçus
        max_number = self.my_election_number
        winner_temp_id = self.process.temp_id

        # Comparer avec tous les nombres reçus
        for other_temp_id, other_number in self.received_election_numbers.items():
            if other_number > max_number:
                max_number = other_number
                winner_temp_id = other_temp_id
            elif other_number == max_number and other_temp_id < winner_temp_id:
                # En cas d'égalité, le plus petit temp_id gagne (départage déterministe)
                winner_temp_id = other_temp_id

        # Vérifier s'il y a égalité avec mon nombre
        tied_numbers = []
        for other_temp_id, other_number in self.received_election_numbers.items():
            if other_number == self.my_election_number and other_temp_id != self.process.temp_id:
                tied_numbers.append(other_temp_id)

        # S'il y a égalité, relancer l'élection
        if tied_numbers and max_number == self.my_election_number:
            # Relancer silencieusement en cas d'égalité
            sleep(0.5)  # Petit délai pour éviter la synchronisation parfaite
            self.election_in_progress = False
            self.received_election_numbers.clear()  # Nettoyer pour la nouvelle élection
            self._start_simple_election()
            return

        # Déterminer si je suis le gagnant
        if winner_temp_id == self.process.temp_id:
            self.logger.log(f"Won election with number {self.my_election_number}")
            self._become_leader()
        else:
            # Perdant silencieux
            self._wait_for_id_assignment()

        # Nettoyer les données d'élection après un délai (sauf si je suis le leader)
        if winner_temp_id != self.process.temp_id:
            sleep(3.0)  # Attendre que le leader finisse d'assigner les IDs
            self.received_election_numbers.clear()
            self.received_pongs.clear()

    def _become_leader(self):
        """Devenir le coordinateur et assigner les IDs à tous."""
        self.process.myId = 0
        self.process.is_coordinator = True
        self.process.id_assigned = True
        self.process.next_id_to_assign = 1
        self.process.name = f"MainThread-P{self.process.myId}"

        self.logger.log("Election successful - became coordinator", coordinator_only=True)
        self.election_in_progress = False

        # Démarrer le système de heartbeat
        if hasattr(self.process, 'com') and self.process.com:
            self.process.com.start_heartbeat()

        # Assigner immédiatement les IDs à tous les participants de l'élection
        sleep(0.5)  # Petit délai pour stabilité
        participants = list(self.received_election_numbers.keys())

        # Retirer les doublons et trier
        unique_participants = sorted(set(participants))

        self.logger.log(f"Assigning IDs to participants: {unique_participants}", coordinator_only=True)

        # Assigner les IDs dans l'ordre
        for participant_temp_id in unique_participants:
            self.clock.increment()
            id_msg = IdAssignmentMessage.new_id_assignment_message(
                participant_temp_id, self.process.next_id_to_assign,
                self.clock.get_time(), self.process.myId
            )
            id_msg.broadcast(PyBus.Instance())
            self.logger.log(f"Assigned ID {self.process.next_id_to_assign} to temp_id {participant_temp_id}", coordinator_only=True)

            # Enregistrer ce processus
            if hasattr(self.process, 'com') and self.process.com and self.process.com.heartbeat_manager:
                self.process.com.heartbeat_manager.active_processes.add(self.process.next_id_to_assign)

            self.process.next_id_to_assign += 1
            sleep(0.1)  # Petit délai entre assignations

        # Mettre à jour le nombre de processus
        if hasattr(self.process, 'com') and self.process.com:
            self.process.com.nbProcess = self.process.next_id_to_assign
            self.logger.log(f"Updated process count to {self.process.com.nbProcess}", coordinator_only=True)

            # Broadcaster le nombre de processus
            self.clock.increment()
            count_msg = ProcessCountUpdateMessage.new_process_count_update_message(
                self.process.com.nbProcess, self.clock.get_time(), self.process.myId
            )
            count_msg.broadcast(PyBus.Instance())

    def _wait_for_id_assignment(self):
        """Attendre qu'un coordinateur assigne un ID."""
        # Le leader va envoyer les IDs automatiquement, juste attendre
        max_wait = 40  # 4 secondes pour laisser le temps au leader d'envoyer tous les IDs
        wait_count = 0
        while not self.process.id_assigned and wait_count < max_wait:
            sleep(0.1)
            wait_count += 1

        if not self.process.id_assigned:
            # Si toujours pas d'ID, demander explicitement
            self.logger.log("No ID received from election, requesting from coordinator")
            self.clock.increment()
            request_msg = IdRequestMessage.new_id_request_message(self.clock.get_time(), self.process.temp_id)
            request_msg.broadcast(PyBus.Instance())

            # Attendre encore
            wait_count = 0
            while not self.process.id_assigned and wait_count < 30:
                sleep(0.1)
                wait_count += 1

            if not self.process.id_assigned:
                # Vraiment un problème, relancer élection
                self.election_in_progress = False
                self._start_simple_election()

    def _request_id_from_existing_leader(self):
        """Demander un ID au leader existant."""
        self.clock.increment()
        request_msg = IdRequestMessage.new_id_request_message(self.clock.get_time(), self.process.temp_id)
        request_msg.broadcast(PyBus.Instance())

        # Attendre la réponse
        max_wait = 30
        wait_count = 0
        while not self.process.id_assigned and wait_count < max_wait:
            sleep(0.1)
            wait_count += 1

        if not self.process.id_assigned:
            # Relancer élection silencieusement
            self.election_in_progress = False
            self._start_simple_election()

    def handle_election_start(self, event):
        """Handler pour les messages d'élection (non utilisé dans cette version)."""
        pass

    def handle_id_request(self, event):
        """Traite une demande d'ID (coordinateur seulement)."""
        if not self.process.is_coordinator:
            return

        if event.source == self.process.temp_id:
            return  # Ignore notre propre demande

        # Assigner le prochain ID disponible
        assigned_id = self.process.next_id_to_assign
        self.process.next_id_to_assign += 1

        # Envoyer la réponse
        self.clock.increment()
        response = IdAssignmentMessage.new_id_assignment_message(
            assigned_id, self.clock.get_time(), event.source, self.process.myId
        )
        response.broadcast(PyBus.Instance())

        # Broadcaster le nouveau nombre de processus
        new_process_count = self.process.next_id_to_assign  # Le nombre total de processus actuels
        self.clock.increment()
        count_update = ProcessCountUpdateMessage.new_process_count_update_message(
            new_process_count, self.clock.get_time(), self.process.myId
        )
        count_update.broadcast(PyBus.Instance())

        self.logger.log(f"Assigned ID {assigned_id} to temp_id {event.source}", coordinator_only=True)
        self.logger.log(f"Updated process count to {new_process_count}", coordinator_only=True)

    def handle_id_assignment(self, event):
        """Traite la réception d'un ID assigné."""
        if event.dest != self.process.temp_id:
            return

        if self.process.id_assigned:
            return  # Déjà un ID

        # Accepter l'ID assigné
        self.process.myId = event.assigned_id
        self.process.id_assigned = True
        self.process.name = f"MainThread-P{self.process.myId}"

        self.logger.log(f"Received assigned ID {self.process.myId}")
        self.election_in_progress = False

        # Démarrer le système de heartbeat
        if hasattr(self.process, 'com') and self.process.com:
            self.process.com.start_heartbeat()