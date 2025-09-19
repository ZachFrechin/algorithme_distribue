from threading import Lock
from Message import *
from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode
from time import sleep

from middleware.Clock import LogicalClock
from middleware.Logger import ProcessLogger
from middleware.ElectionManager import ElectionManager
from middleware.SyncCommunication import SyncCommunication
from middleware.MailBoxManager import MailBoxManager
from middleware.HeartbeatManager import HeartbeatManager
from middleware.Errors.MiddlewareException import *

class Com:
    def __init__(self, process = None):
        self.process = process

        self.logical_clock = LogicalClock()
        self.logger = ProcessLogger(process, self.logical_clock) if process else None
        self.election_manager = ElectionManager(process, self.logical_clock, self.logger) if process else None
        self.sync_comm = SyncCommunication(process, self.logical_clock, self.logger) if process else None
        self.mailbox_manager = MailBoxManager(self.logical_clock)
        self.heartbeat_manager = HeartbeatManager(process, self.logical_clock, self.logger) if process else None

        self.token = None
        self.token_mutex = Lock()
        self.SC = 0

        self.nbProcess = 0
        self.sync_ready_count = 0
        self.sync_barrier = False
        self.sync_generation = 0
        self.ready_sent_for_generation = set()

        PyBus.Instance().register(self, self)

    def log(self, message, coordinator_only=False):
        if self.logger:
            self.logger.log(message, coordinator_only)

    def inc_clock(self):
        return self.logical_clock.increment()

    def get_clock(self):
        return self.logical_clock.get_time()

    def _update_clock_on_receive(self, clock):
        return self.logical_clock.update_on_receive(clock)

    def put_message(self, message: Message):
        """Délègue au gestionnaire de mailbox."""
        if not message.USER_MESSAGE:
            raise NotUserMessageException(self.process.myId, self.get_clock())
        self.mailbox_manager.put_message(message)

    def get_message(self):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_message()

    def get_messages(self):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_messages()

    def get_message_from(self, source):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_message_from(source)

    def get_messages_from(self, source):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_messages_from(source)

    def has_messages(self):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.has_messages()

    def get_messages_type(self, message_type=None):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_messages_type(message_type)

    def get_message_type(self, message_type=None):
        """Délègue au gestionnaire de mailbox."""
        return self.mailbox_manager.get_message_type(message_type)

    def broadcast(self, payload):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = BroadcastMessage.new_broadcast_message(payload, self.get_clock(), self.process.myId)
        message.broadcast(PyBus.Instance())

    def send_to(self, payload, dest):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = DedicatedMessage.new_dedicated_message(payload, self.get_clock(), dest, self.process.myId)
        message.send(PyBus.Instance())

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastMessage)
    def on_broadcast(self, event):
        # Filtrer nos propres messages (seulement après assignation d'ID)
        if self.process.id_assigned and event.source == self.process.myId:
            return

        # Pendant l'attribution d'ID, filtrer nos messages temp_id
        if not self.process.id_assigned and event.source == self.process.temp_id:
            return

        # Gérer les messages système (ne vont PAS dans la mailbox utilisateur)
        payload = event.get_payload()

        # Messages d'élection
        if payload.startswith("ELECTION_NUMBER:"):
            # Transmettre à l'ElectionManager
            if self.election_manager:
                try:
                    parts = payload.split(":")
                    temp_id = int(parts[1])
                    election_number = int(parts[2])
                    self.election_manager.receive_election_number(temp_id, election_number)
                except (ValueError, IndexError):
                    pass
            return

        if payload.startswith("ELECTION_START:") and not self.process.id_assigned:
            if self.election_manager:
                self.election_manager.handle_election_start(event)
            return

        # Messages de coordination
        if payload == "PING_COORDINATOR":
            if self.process.is_coordinator:
                self.inc_clock()
                response = BroadcastMessage.new_broadcast_message(f"PONG_COORDINATOR:{self.process.myId}", self.get_clock(), self.process.myId)
                response.broadcast(PyBus.Instance())
            return  # Toujours ignorer, même si pas coordinateur

        if payload.startswith("PONG_COORDINATOR:"):
            # Transmettre à l'ElectionManager
            if self.election_manager:
                try:
                    coordinator_id = int(payload.split(":")[1])
                    self.election_manager.receive_pong_coordinator(coordinator_id)
                except (ValueError, IndexError):
                    pass
            return

        if payload.startswith("PROCESS_COUNT_UPDATE:"):
            # Traité directement par le handler, pas dans la mailbox
            return

        # Seuls les messages utilisateur vont dans la mailbox
        self.put_message(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=DedicatedMessage)
    def on_dedicated(self, event):
        """Handler pour les messages dédiés (point-à-point)."""
        # Vérifier si c'est pour nous
        if event.dest != self.process.myId:
            return

        # Filtrer nos propres messages
        if event.source == self.process.myId:
            return

        # Message destiné à ce processus, le mettre dans la mailbox
        self.put_message(event)

    def request_SC(self):
        """Demande d'accès à la section critique - bloque jusqu'à obtention du token"""
        self.SC = 1
        self.log("Requesting critical section access")

        # Attendre d'avoir le token ET d'être marqué comme en section critique
        while self.SC != 2:
            sleep(0.01)

        self.log("Now in critical section")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=TokenMessage)
    def on_token(self, event):
        sleep(1.0)  # Délai d'1 seconde pour mieux visualiser
        if not self.process.alive:
            return

        if event.source == self.process.myId:
            return

        if event.dest != self.process.myId:
            return

        # Token reçu
        self.log(f"Received token from P{event.source}")
        self.token = event.get_token()

        if self.SC == 1:
            self.log("Keeping token for critical section")
            self.log("Entered critical section")
            self.SC = 2

        while self.SC != 0:
            sleep(0.01)

        next_process = self.process.next_node()
        self.log(f"Forwarding token to P{next_process}")
        self.send_token(event.get_token())
        self.SC = 0

    def release_SC(self):
        self.log("Exiting critical section")
        self.SC = 0

    def send_token(self, token=None):
        next_process = self.process.next_node()

        if token is not None:
            # Transférer un token reçu
            self.log(f"Sending token to P{next_process}")
            TokenMessage.new_token_message(token, self.get_clock(), next_process, self.process.myId).send(PyBus.Instance())
        else:
            # Envoyer mon token
            self.log(f"Releasing my token to P{next_process}")
            TokenMessage.new_token_message(self.token, self.get_clock(), next_process, self.process.myId).send(PyBus.Instance())
            self.token = None

        # Délai pour mieux visualiser le passage de token
        sleep(0.5)

    def send_sync_message(self, msg_type):
        # Ne pas envoyer de messages sync si pas d'ID assigné
        if not self.process.id_assigned or self.process.myId is None:
            return

        # Inclure la génération dans le message pour éviter les anciennes barrières
        message_with_gen = f"{msg_type}:{self.sync_generation}"
        SyncMessage.new_sync_message(message_with_gen, self.get_clock(), self.process.myId).send(PyBus.Instance())

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SyncMessage)
    def on_sync(self, event):
        msg_type_full = event.get_type()

        # Extraire le type et la génération
        if ":" in msg_type_full:
            msg_type, generation_str = msg_type_full.split(":", 1)
            try:
                generation = int(generation_str)
            except ValueError:
                generation = 0
        else:
            msg_type = msg_type_full
            generation = 0

        # Ignorer les messages d'anciennes générations
        if generation < self.sync_generation:
            return

        if msg_type == "READY":
            # Compter notre propre READY aussi
            if event.source == self.process.myId:
                self.sync_ready_count += 1
                return

            # Ignorer les READY des processus non-assignés
            if event.source is None:
                return

            # READY des autres processus
            # Si on reçoit un READY et qu'on n'est pas déjà en synchronisation, on rejoint automatiquement
            if not self.sync_barrier:
                # Vérifier si on a déjà envoyé READY pour cette génération
                if generation in self.ready_sent_for_generation:
                    return  # On a déjà participé à cette génération

                # Utiliser la génération du message reçu
                self.sync_generation = generation
                self.log(f"Auto-joining sync barrier gen {generation} (triggered by P{event.source})")
                self.sync_barrier = True
                self.sync_ready_count = 0  # Réinitialiser le compteur

                # Marquer qu'on envoie READY pour cette génération
                self.ready_sent_for_generation.add(generation)
                self.send_sync_message("READY")
                return  # Important: sortir après avoir envoyé notre READY

            self.sync_ready_count += 1
            # Obtenir le nombre de processus actifs du heartbeat
            active_count = len(self.heartbeat_manager.get_active_processes()) if self.heartbeat_manager else self.nbProcess
            expected_ready = active_count  # Tous les processus doivent envoyer READY

            # Synchronisation - logs optimisés pour le coordinateur
            if self.process.is_coordinator:
                if self.sync_ready_count == 1:
                    self.log(f"Sync barrier: waiting for {expected_ready} processes", coordinator_only=True)
                elif self.sync_ready_count == expected_ready:
                    self.log(f"Sync barrier: all {expected_ready} processes ready, releasing", coordinator_only=True)

            if self.sync_ready_count >= expected_ready and self.sync_barrier:
                self._update_clock_on_receive(event.get_stamp())
                self.send_sync_message("SYNC_RELEASE")
                self.sync_ready_count = 0

        elif msg_type == "SYNC_RELEASE" and self.sync_barrier:
            self._update_clock_on_receive(event.get_stamp())
            self.sync_barrier = False
            self.sync_ready_count = 0

            # Nettoyer les anciennes générations (garder seulement les 5 dernières)
            if len(self.ready_sent_for_generation) > 5:
                # Supprimer les plus anciennes générations
                sorted_generations = sorted(self.ready_sent_for_generation)
                old_generations = sorted_generations[:-5]
                for old_gen in old_generations:
                    self.ready_sent_for_generation.discard(old_gen)

    def synchronize(self):
        # Ne pas synchroniser si pas d'ID assigné
        if not self.process.id_assigned or self.process.myId is None:
            self.log("Cannot synchronize: no ID assigned")
            return

        # Nouvelle génération de barrière pour éviter les conflits
        self.sync_generation += 1

        # Vérifier si on a déjà envoyé READY pour cette génération
        if self.sync_generation in self.ready_sent_for_generation:
            self.log(f"Already sent READY for generation {self.sync_generation}, skipping")
            return

        self.log(f"Entering synchronization barrier (gen {self.sync_generation})")
        self.sync_barrier = True
        self.sync_ready_count = 0  # Réinitialiser le compteur

        # Marquer qu'on envoie READY pour cette génération
        self.ready_sent_for_generation.add(self.sync_generation)
        self.send_sync_message("READY")

        while self.sync_barrier:
            sleep(0.01)

        self.log("Passed synchronization barrier")

    def broadcastSync(self, payload, sender_id):
        if self.sync_comm:
            self.sync_comm.broadcast_sync(payload, sender_id)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastSyncMessage)
    def on_broadcast_sync(self, event):
        if self.sync_comm:
            self.sync_comm.handle_broadcast_sync(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastSyncAckMessage)
    def on_broadcast_sync_ack(self, event):
        if self.sync_comm:
            self.sync_comm.handle_broadcast_sync_ack(event)

    def sendToSync(self, payload, dest):
        if self.sync_comm:
            self.sync_comm.send_to_sync(payload, dest)

    def recevFromSync(self, source):
        if self.sync_comm:
            return self.sync_comm.receive_from_sync(source)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SendToSyncMessage)
    def on_send_to_sync(self, event):
        if self.sync_comm:
            self.sync_comm.handle_send_to_sync(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SendToSyncAckMessage)
    def on_send_to_sync_ack(self, event):
        if self.sync_comm:
            self.sync_comm.handle_send_to_sync_ack(event)

    def request_dynamic_id(self):
        if self.election_manager:
            self.election_manager.request_dynamic_id()

    @subscribe(threadMode = Mode.PARALLEL, onEvent=IdRequestMessage)
    def on_id_request(self, event):
        if self.election_manager:
            self.election_manager.handle_id_request(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=IdAssignmentMessage)
    def on_id_assignment(self, event):
        if self.election_manager:
            self.election_manager.handle_id_assignment(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=ProcessCountUpdateMessage)
    def on_process_count_update(self, event):
        """Met à jour le nombre de processus."""
        if event.source != self.process.myId:  # Pas notre propre message
            old_count = self.nbProcess
            self.nbProcess = event.new_count
            self.log(f"Process count updated from {old_count} to {self.nbProcess}")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=HeartbeatMessage)
    def on_heartbeat(self, event):
        """Traite la réception d'un heartbeat."""
        if self.heartbeat_manager:
            self.heartbeat_manager.handle_heartbeat(event)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=IdReassignmentMessage)
    def on_id_reassignment(self, event):
        """Traite la réassignation d'ID après panne."""
        # Vérifier si c'est pour nous
        if event.old_id == self.process.myId:
            old_id = self.process.myId
            new_id = event.new_id

            # Mettre à jour notre ID
            self.process.myId = new_id
            self.log(f"Reassigned from P{old_id} to P{new_id}")

    def start_heartbeat(self):
        """Démarre le système de heartbeat (appelé après assignation d'ID)."""
        if self.heartbeat_manager:
            self.heartbeat_manager.start()

    def stop_heartbeat(self):
        """Arrête le système de heartbeat."""
        if self.heartbeat_manager:
            self.heartbeat_manager.stop()