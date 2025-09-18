from threading import Lock
from Message import Message, BroadcastMessage, DedicatedMessage, RegisterMessage, RegistrationMessage, RequestMaxIdMessage, ResponseMaxIdMessage, TokenMessage, SyncMessage, BroadcastSyncMessage, BroadcastSyncAckMessage, SendToSyncMessage, SendToSyncAckMessage, IdRequestMessage, IdAssignmentMessage, HeartbeatMessage
from pyeventbus3.pyeventbus3 import PyBus, subscribe, Mode
from time import sleep
import uuid


class Com:
    def __init__(self, process = None):
        self.process = process
        self.clock = 0
        self._clock_mutex = Lock()
        self._mail_box_mutex = Lock()
        self.mail_box = []
        self.token = None
        self.token_mutex = Lock()
        self.SC = 0

        self.nbProcess = 0
        self.sync_ready_count = 0
        self.sync_barrier = False

        # Variables pour communications synchrones
        self.pending_broadcast_acks = {}  # msg_id -> set of pending process ids
        self.pending_sendto_acks = {}     # msg_id -> expected process id
        self.received_sync_messages = {}  # (from, msg_id) -> message content
        self.message_counter = 0

        PyBus.Instance().register(self, self)

    def inc_clock(self):
        with self._clock_mutex:
            self.clock += 1
            return self.clock

    def get_clock(self):
        with self._clock_mutex:
            return self.clock

    def _update_clock_on_receive(self, clock):
        with self._clock_mutex:
            self.clock = max(self.clock, clock) + 1
            return self.clock

    def _set_clock(self, clock):
        with self._clock_mutex:
            self.clock = clock

    def put_message(self, message : Message):
        if not message.USER_MESSAGE:
            raise TypeError("Only user messages can be put in the mail box")
        with self._mail_box_mutex:
            self.mail_box.append(message)
            self._update_clock_on_receive(message.stamp)

    def get_message(self):
        with self._mail_box_mutex:
            return self.mail_box.pop(0)

    def get_messages(self):
        with self._mail_box_mutex:
            messages = self.mail_box
            self.mail_box = []
            return messages

    def get_message_from(self, source):
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return None
            self.mail_box.remove(messages[0])
            return messages[0]

    def get_messages_from(self, source):
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return []
            self.mail_box = [message for message in self.mail_box if message.source != source]
            return messages

    def has_messages(self):
        with self._mail_box_mutex:
            return len(self.mail_box) > 0

    def get_messages_type(self, type = None):
        if type is None:
            return None

        with self._mail_box_mutex:
            match type:
                case "broadcast" :
                    messages = [message for message in self.mail_box if isinstance(message, BroadcastMessage)]
                case "dedicated" :
                    messages = [message for message in self.mail_box if isinstance(message, DedicatedMessage)]
                case _:
                    return None
            return messages

    def get_message_type(self, type = None):
        if type is None:
            return None
        messages = self.get_messages_type(type)
        if messages is None or len(messages) == 0:
            return None
        return messages[0]


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

    def register(self, payload):
        if self.process is None:
            raise ValueError("Process is not set")
        self.inc_clock()
        message = RegisterMessage.new_register_message(payload, self.get_clock(), self.process.myId)
        message.broadcast(PyBus.Instance())

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastMessage)
    def on_broadcast(self, event):
        if event.source == self.process.myId:
            return

        self.put_message(event)

    def request_SC(self):
        """Demande d'accès à la section critique - bloque jusqu'à obtention du token"""
        self.SC = 1
        print(f"{self.process.name} requesting critical section access...")

        while self.token is None and self.SC == 1:
            sleep(0.01)  # Attente moins intensive

        if self.SC == 1:
            print(f"{self.process.name} ✓ entered critical section with token: {self.token}")
            self.SC = 2  # Marquer en section critique

    @subscribe(threadMode = Mode.PARALLEL, onEvent=TokenMessage)
    def on_token(self, event):
        sleep(0.2)
        if not self.process.alive:
            return

        if event.source == self.process.myId:
            return

        if event.dest != self.process.myId:
            return 

        print(f"{self.process.name} ← received token from P{event.source}")

        if self.SC == 1:
            print(f"{self.process.name} ⚡ acquired token for critical section")
            self.token = event.get_token()
        else:  # Pas de demande, passer le token
            print(f"{self.process.name} → forwarding token to P{self.process.next_node()}")
            self.send_token(event.get_token())
            self.SC = 0

    def release_SC(self):
        print(f"{self.process.name} ✗ exiting critical section")
        self.SC = 0  # Fin de section critique
        self.send_token()

    def send_token(self, token=None):
        next_process = self.process.next_node()

        if token is not None:
            # Transférer un token reçu
            print(f"{self.process.name} → passing token to P{next_process}")
            TokenMessage.new_token_message(token, self.get_clock(), next_process, self.process.myId).send(PyBus.Instance())
        else:
            # Envoyer mon token
            print(f"{self.process.name} → releasing token to P{next_process}")
            TokenMessage.new_token_message(self.token, self.get_clock(), next_process, self.process.myId).send(PyBus.Instance())
            self.token = None



    def send_sync_message(self, msg_type):
        SyncMessage.new_sync_message(msg_type, self.get_clock(), self.process.myId).send(PyBus.Instance())

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SyncMessage)
    def on_sync(self, event):
        msg_type = event.get_type()

        if msg_type == "READY" and event.source != self.process.myId:
            self.sync_ready_count += 1
            print(f"{self.process.name} received READY from process {event.source} ({self.sync_ready_count}/{self.nbProcess})")

            if self.sync_ready_count == self.nbProcess and self.sync_barrier:
                self._update_clock_on_receive(event.get_stamp())
                self.send_sync_message("SYNC_RELEASE")
                self.sync_ready_count = 0

        elif msg_type == "SYNC_RELEASE" and self.sync_barrier:
            self._update_clock_on_receive(event.get_stamp())
            self.sync_barrier = False
            self.sync_ready_count = 0

    def synchronize(self):
        self.sync_ready_count += 1
        print(f"{self.process.name} entering synchronization barrier")
        self.sync_barrier = True

        self.send_sync_message("READY")

        while self.sync_barrier:
            sleep(0.01)

        print(f"{self.process.name} passed synchronization barrier, clock: {self.clock}")

    def broadcastSync(self, payload, sender_id):
        """
        Communication broadcast synchrone bloquante.

        Args:
            payload: L'objet à diffuser
            sender_id: L'ID du processus émetteur

        Si le processus courant a l'ID 'sender_id', il envoie le message à tous
        et attend les accusés de réception de tous les autres processus.
        Sinon, il attend de recevoir le message de 'sender_id'.
        """
        if self.process is None:
            raise ValueError("Process is not set")

        if self.process.myId == sender_id:
            # Je suis l'émetteur
            self.message_counter += 1
            msg_id = f"{self.process.myId}-{self.message_counter}"

            # Créer la liste des processus qui doivent accuser réception
            expected_acks = set(range(self.nbProcess)) - {self.process.myId}
            self.pending_broadcast_acks[msg_id] = expected_acks

            print(f"{self.process.name} broadcasting sync message: {payload}")
            self.inc_clock()
            message = BroadcastSyncMessage.new_broadcast_sync_message(
                payload, self.get_clock(), sender_id, msg_id, self.process.myId
            )
            message.broadcast(PyBus.Instance())

            # Attendre tous les accusés de réception
            while msg_id in self.pending_broadcast_acks and len(self.pending_broadcast_acks[msg_id]) > 0:
                sleep(0.01)

            # Nettoyer
            if msg_id in self.pending_broadcast_acks:
                del self.pending_broadcast_acks[msg_id]

            print(f"{self.process.name} broadcast sync completed, all processes received")

        else:
            # Je suis un récepteur, attendre le message de sender_id
            msg_key = (sender_id, None)  # On ne connaît pas encore le msg_id

            while True:
                # Chercher si on a reçu un message de sender_id
                found_message = None
                for (src, mid), msg in self.received_sync_messages.items():
                    if src == sender_id:
                        found_message = msg
                        msg_key = (src, mid)
                        break

                if found_message:
                    del self.received_sync_messages[msg_key]
                    print(f"{self.process.name} received broadcast sync from P{sender_id}: {found_message}")
                    break

                sleep(0.01)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastSyncMessage)
    def on_broadcast_sync(self, event):
        """Handler pour les messages broadcast synchrones"""
        if event.source == self.process.myId:
            return

        # Stocker le message reçu
        msg_key = (event.get_sender(), event.get_msg_id())
        self.received_sync_messages[msg_key] = event.get_payload()

        # Envoyer accusé de réception
        self.inc_clock()
        ack = BroadcastSyncAckMessage.new_broadcast_sync_ack_message(
            event.get_msg_id(), self.get_clock(), event.get_sender(), self.process.myId
        )
        ack.send(PyBus.Instance())

        print(f"{self.process.name} sent ack for broadcast sync message {event.get_msg_id()}")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastSyncAckMessage)
    def on_broadcast_sync_ack(self, event):
        """Handler pour les accusés de réception de broadcast synchrone"""
        if event.source == self.process.myId:
            return

        msg_id = event.get_msg_id()

        # Retirer ce processus de la liste d'attente
        if msg_id in self.pending_broadcast_acks:
            if event.source in self.pending_broadcast_acks[msg_id]:
                self.pending_broadcast_acks[msg_id].remove(event.source)
                print(f"{self.process.name} received ack from P{event.source} for message {msg_id}")

    def sendToSync(self, payload, dest):
        """
        Envoie un message à un processus spécifique de manière synchrone.

        Args:
            payload: L'objet à envoyer
            dest: L'ID du processus destinataire

        Bloque jusqu'à ce que le destinataire ait reçu le message.
        """
        if self.process is None:
            raise ValueError("Process is not set")

        self.message_counter += 1
        msg_id = f"{self.process.myId}-{self.message_counter}"

        # Enregistrer qu'on attend un ack de 'dest'
        self.pending_sendto_acks[msg_id] = dest

        print(f"{self.process.name} sending sync message to P{dest}: {payload}")
        self.inc_clock()
        message = SendToSyncMessage.new_send_to_sync_message(
            payload, self.get_clock(), dest, msg_id, self.process.myId
        )
        message.send(PyBus.Instance())

        # Attendre l'accusé de réception
        while msg_id in self.pending_sendto_acks:
            sleep(0.01)

        print(f"{self.process.name} send sync to P{dest} completed")

    def recevFromSync(self, source):
        """
        Attend de recevoir un message d'un processus spécifique de manière synchrone.

        Args:
            source: L'ID du processus émetteur

        Returns:
            Le payload du message reçu

        Bloque jusqu'à recevoir un message de 'source'.
        """
        while True:
            # Chercher si on a reçu un message de source
            found_message = None
            for (src, mid), msg in self.received_sync_messages.items():
                if src == source:
                    found_message = msg
                    msg_key = (src, mid)
                    break

            if found_message:
                del self.received_sync_messages[msg_key]
                print(f"{self.process.name} received sync message from P{source}: {found_message}")
                return found_message

            sleep(0.01)

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SendToSyncMessage)
    def on_send_to_sync(self, event):
        """Handler pour les messages point-à-point synchrones"""
        if event.source == self.process.myId:
            return

        if event.get_dest() != self.process.myId:
            return

        # Stocker le message reçu
        msg_key = (event.source, event.get_msg_id())
        self.received_sync_messages[msg_key] = event.get_payload()

        # Envoyer accusé de réception
        self.inc_clock()
        ack = SendToSyncAckMessage.new_send_to_sync_ack_message(
            event.get_msg_id(), self.get_clock(), event.source, self.process.myId
        )
        ack.send(PyBus.Instance())

        print(f"{self.process.name} sent ack for sync message {event.get_msg_id()}")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=SendToSyncAckMessage)
    def on_send_to_sync_ack(self, event):
        """Handler pour les accusés de réception point-à-point synchrones"""
        if event.source == self.process.myId:
            return

        if event.get_original_sender() != self.process.myId:
            return

        msg_id = event.get_msg_id()

        # Retirer ce message de la liste d'attente
        if msg_id in self.pending_sendto_acks:
            del self.pending_sendto_acks[msg_id]
            print(f"{self.process.name} received ack from P{event.source} for message {msg_id}")

    def initiate_id_assignment(self):
        """
        Démarre le processus d'attribution automatique des IDs.
        Le processus avec le plus petit temp_id devient coordinateur.
        """
        print(f"{self.process.name} initiating ID assignment with temp_id: {self.process.temp_id}")

        # Attendre un peu pour laisser tous les processus démarrer
        sleep(1)

        # Broadcaster notre temp_id pour l'élection du coordinateur avec temp_id comme source
        self.inc_clock()
        message = BroadcastMessage.new_broadcast_message(f"TEMP_ID:{self.process.temp_id}", self.get_clock(), self.process.temp_id)
        message.broadcast(PyBus.Instance())

        # Attendre de collecter tous les temp_ids
        sleep(2)

        # Vérifier si on est le coordinateur (plus petit temp_id)
        min_temp_id = self.process.temp_id

        # Regarder dans les messages reçus
        broadcast_messages = self.get_messages_type("broadcast")
        for msg in broadcast_messages:
            if msg.get_payload().startswith("TEMP_ID:"):
                other_temp_id = int(msg.get_payload().split(":")[1])
                if other_temp_id < min_temp_id:
                    min_temp_id = other_temp_id

        if min_temp_id == self.process.temp_id:
            # Je suis le coordinateur
            self.process.is_coordinator = True
            self.process.myId = 0
            self.process.id_assigned = True
            print(f"{self.process.name} elected as coordinator with ID 0")

            # Assigner des IDs aux autres processus
            self.assign_ids_to_others()
        else:
            # Demander un ID au coordinateur
            self.request_id_from_coordinator()

    def assign_ids_to_others(self):
        """
        En tant que coordinateur, assigner des IDs aux autres processus.
        """
        current_id = 1  # Le coordinateur a déjà l'ID 0

        # Collecter tous les processus qui ont envoyé leur temp_id
        temp_ids_processes = []
        broadcast_messages = self.get_messages_type("broadcast")

        for msg in broadcast_messages:
            if msg.get_payload().startswith("TEMP_ID:"):
                temp_id = int(msg.get_payload().split(":")[1])
                if temp_id != self.process.temp_id:  # Pas nous-même
                    temp_ids_processes.append((temp_id, msg.source))

        # Trier par temp_id pour assigner les IDs de manière déterministe
        temp_ids_processes.sort()

        for temp_id, source in temp_ids_processes:
            self.inc_clock()
            assignment_msg = IdAssignmentMessage.new_id_assignment_message(
                current_id, self.get_clock(), source, 0  # Le coordinateur a l'ID 0
            )
            assignment_msg.send(PyBus.Instance())
            print(f"{self.process.name} assigned ID {current_id} to process {source}")
            current_id += 1

    def request_id_from_coordinator(self):
        """
        Demander un ID au processus coordinateur.
        """
        self.inc_clock()
        request_msg = IdRequestMessage.new_id_request_message(
            self.get_clock(), self.process.temp_id  # Utiliser temp_id avant assignation
        )
        request_msg.broadcast(PyBus.Instance())
        print(f"{self.process.name} requesting ID from coordinator")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=IdRequestMessage)
    def on_id_request(self, event):
        """Handler pour les demandes d'ID"""
        if not self.process.is_coordinator:
            return

        if event.source == self.process.myId:
            return

        # Assigner le prochain ID disponible
        assigned_id = self.process.next_id_to_assign
        self.process.next_id_to_assign += 1

        self.inc_clock()
        assignment_msg = IdAssignmentMessage.new_id_assignment_message(
            assigned_id, self.get_clock(), event.source, self.process.myId
        )
        assignment_msg.send(PyBus.Instance())

        print(f"{self.process.name} (coordinator) assigned ID {assigned_id} to process {event.source}")

    @subscribe(threadMode = Mode.PARALLEL, onEvent=IdAssignmentMessage)
    def on_id_assignment(self, event):
        """Handler pour les assignations d'ID"""
        if event.get_dest() != self.process.temp_id:  # Comparer avec temp_id
            return

        if self.process.id_assigned:
            return  # Déjà assigné

        self.process.myId = event.get_assigned_id()
        self.process.id_assigned = True

        print(f"{self.process.name} received assigned ID: {self.process.myId}")

        # Mettre à jour le nom du processus avec le nouvel ID
        self.process.name = f"MainThread-P{self.process.myId}"

    def start_heartbeat(self):
        """
        Démarre le système de heartbeat pour détection de pannes.
        Envoie périodiquement un heartbeat et surveille ceux des autres processus.
        """
        import threading
        from time import time

        def heartbeat_sender():
            while self.process.alive:
                if self.process.id_assigned:
                    self.inc_clock()
                    heartbeat_msg = HeartbeatMessage.new_heartbeat_message(
                        self.get_clock(), self.process.myId
                    )
                    heartbeat_msg.broadcast(PyBus.Instance())
                    print(f"{self.process.name} sent heartbeat")

                sleep(self.process.heartbeat_interval)

        def failure_detector():
            while self.process.alive:
                if self.process.id_assigned:
                    current_time = time()
                    failed_processes = []

                    for process_id in list(self.process.last_heartbeat.keys()):
                        last_seen = self.process.last_heartbeat[process_id]
                        if current_time - last_seen > self.process.failure_timeout:
                            failed_processes.append(process_id)

                    for failed_id in failed_processes:
                        print(f"{self.process.name} detected failure of process P{failed_id}")
                        self.handle_process_failure(failed_id)

                sleep(1)  # Vérifier toutes les secondes

        # Démarrer les threads de heartbeat
        threading.Thread(target=heartbeat_sender, daemon=True).start()
        threading.Thread(target=failure_detector, daemon=True).start()

    def handle_process_failure(self, failed_id):
        """
        Gère la défaillance d'un processus détecté.
        """
        if failed_id in self.process.active_processes:
            self.process.active_processes.remove(failed_id)

        if failed_id in self.process.last_heartbeat:
            del self.process.last_heartbeat[failed_id]

        print(f"{self.process.name} removed P{failed_id} from active processes")

        # Si c'est le coordinateur qui tombe, réorganiser les IDs
        if self.process.is_coordinator and failed_id == 0:
            self.reorganize_ids_after_coordinator_failure()
        elif failed_id < self.process.myId:
            # Un processus avec un ID inférieur est tombé, décaler les IDs
            self.reorganize_ids_after_failure(failed_id)

    def reorganize_ids_after_failure(self, failed_id):
        """
        Réorganise les IDs après la défaillance d'un processus.
        """
        if failed_id < self.process.myId:
            old_id = self.process.myId
            self.process.myId -= 1  # Décaler l'ID vers le bas
            print(f"{self.process.name} ID changed from {old_id} to {self.process.myId} due to P{failed_id} failure")

            # Mettre à jour le nom
            self.process.name = f"MainThread-P{self.process.myId}"

            # Mettre à jour nbProcess
            self.nbProcess -= 1
            self.process.npProcess -= 1

    def reorganize_ids_after_coordinator_failure(self):
        """
        Réorganise le système après la défaillance du coordinateur.
        Le processus avec l'ID le plus petit devient le nouveau coordinateur.
        """
        if not self.process.active_processes:
            return

        min_active_id = min(self.process.active_processes.union({self.process.myId}))

        if self.process.myId == min_active_id:
            # Je deviens le nouveau coordinateur
            old_id = self.process.myId
            self.process.myId = 0
            self.process.is_coordinator = True
            print(f"{self.process.name} became new coordinator (was P{old_id})")

            # Mettre à jour le nom
            self.process.name = f"MainThread-P{self.process.myId}"

    @subscribe(threadMode = Mode.PARALLEL, onEvent=HeartbeatMessage)
    def on_heartbeat(self, event):
        """Handler pour les messages heartbeat"""
        if event.source == self.process.myId:
            return

        from time import time
        current_time = time()

        # Enregistrer ce processus comme actif
        self.process.active_processes.add(event.source)
        self.process.last_heartbeat[event.source] = current_time

        # Mettre à jour l'horloge logique
        self._update_clock_on_receive(event.get_stamp())