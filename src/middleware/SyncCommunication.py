from time import sleep
from Message import BroadcastSyncMessage, BroadcastSyncAckMessage, SendToSyncMessage, SendToSyncAckMessage
from pyeventbus3.pyeventbus3 import PyBus

class SyncCommunication:
    """
    Gestionnaire des communications synchrones (broadcastSync, sendToSync, recevFromSync).
    """

    def __init__(self, process, clock, logger):
        self.process = process
        self.clock = clock
        self.logger = logger

        # Variables pour communications synchrones
        self.pending_broadcast_acks = {}  # msg_id -> set of pending process ids
        self.pending_sendto_acks = {}     # msg_id -> expected process id
        self.received_sync_messages = {}  # (from, msg_id) -> message content
        self.message_counter = 0

    def broadcast_sync(self, payload, sender_id):
        """Communication broadcast synchrone bloquante."""
        if self.process.myId == sender_id:
            # Je suis l'émetteur
            self.message_counter += 1
            msg_id = f"{self.process.myId}-{self.message_counter}"

            # Créer la liste des processus qui doivent accuser réception
            if self.process.com.heartbeat_manager:
                active_processes = set(self.process.com.heartbeat_manager.get_active_processes())
                expected_acks = active_processes - {self.process.myId}
            else:
                expected_acks = set(range(self.process.com.nbProcess)) - {self.process.myId}
            self.pending_broadcast_acks[msg_id] = expected_acks

            self.logger.log(f"Broadcasting sync: {payload}", coordinator_only=True)
            self.clock.increment()
            message = BroadcastSyncMessage.new_broadcast_sync_message(
                payload, self.clock.get_time(), sender_id, msg_id, self.process.myId
            )
            message.broadcast(PyBus.Instance())

            # Attendre tous les accusés de réception
            self.logger.log(f"BroadcastSync: waiting for {len(expected_acks)} acks", coordinator_only=True)
            while msg_id in self.pending_broadcast_acks and len(self.pending_broadcast_acks[msg_id]) > 0:
                sleep(0.1)  # Délai réduit

            # Nettoyer
            if msg_id in self.pending_broadcast_acks:
                del self.pending_broadcast_acks[msg_id]

            self.logger.log(f"Broadcast sync completed", coordinator_only=True)

        else:
            # Je suis un récepteur, attendre le message de sender_id
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
                    self.logger.log(f"Received broadcast sync from P{sender_id}: {found_message}")
                    break

                sleep(0.01)

    def send_to_sync(self, payload, dest):
        """Envoie un message à un processus spécifique de manière synchrone."""
        self.message_counter += 1
        msg_id = f"{self.process.myId}-{self.message_counter}"

        # Enregistrer qu'on attend un ack de 'dest'
        self.pending_sendto_acks[msg_id] = dest

        self.logger.log(f"Sending sync to P{dest}: {payload}")
        self.clock.increment()
        message = SendToSyncMessage.new_send_to_sync_message(
            payload, self.clock.get_time(), dest, msg_id, self.process.myId
        )
        message.send(PyBus.Instance())

        # Attendre l'accusé de réception
        self.logger.log(f"Waiting for acknowledgment from P{dest}")
        while msg_id in self.pending_sendto_acks:
            sleep(0.1)

        self.logger.log(f"Send sync to P{dest} completed")

    def receive_from_sync(self, source):
        """Attend de recevoir un message d'un processus spécifique de manière synchrone."""
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
                self.logger.log(f"Received sync from P{source}: {found_message}")
                return found_message

            sleep(0.01)

    def handle_broadcast_sync(self, event):
        """Handler pour les messages broadcast synchrones."""
        if event.source == self.process.myId:
            return

        # Stocker le message reçu
        msg_key = (event.get_sender(), event.get_msg_id())
        self.received_sync_messages[msg_key] = event.get_payload()

        # Logger la réception du message broadcast sync
        self.logger.log(f"Received broadcastSync from P{event.get_sender()}: {event.get_payload()}")

        # Envoyer accusé de réception silencieusement
        self.clock.increment()
        ack = BroadcastSyncAckMessage.new_broadcast_sync_ack_message(
            event.get_msg_id(), self.clock.get_time(), event.get_sender(), self.process.myId
        )
        ack.send(PyBus.Instance())

    def handle_broadcast_sync_ack(self, event):
        """Handler pour les accusés de réception de broadcast synchrone."""
        if event.source == self.process.myId:
            return

        msg_id = event.get_msg_id()

        # Retirer ce processus de la liste d'attente
        if msg_id in self.pending_broadcast_acks:
            if event.source in self.pending_broadcast_acks[msg_id]:
                self.pending_broadcast_acks[msg_id].remove(event.source)
                remaining = len(self.pending_broadcast_acks[msg_id])
                if remaining == 0:
                    self.logger.log(f"BroadcastSync: all acks received", coordinator_only=True)

    def handle_send_to_sync(self, event):
        """Handler pour les messages point-à-point synchrones."""
        if event.source == self.process.myId:
            return

        if event.get_dest() != self.process.myId:
            return

        # Stocker le message reçu
        msg_key = (event.source, event.get_msg_id())
        self.received_sync_messages[msg_key] = event.get_payload()

        # Logger la réception du message sync
        self.logger.log(f"Received sync message from P{event.source}: {event.get_payload()}")

        # Envoyer accusé de réception
        self.clock.increment()
        ack = SendToSyncAckMessage.new_send_to_sync_ack_message(
            event.get_msg_id(), self.clock.get_time(), event.source, self.process.myId
        )
        self.logger.log(f"Sending acknowledgment to P{event.source}")
        ack.send(PyBus.Instance())

    def handle_send_to_sync_ack(self, event):
        """Handler pour les accusés de réception point-à-point synchrones."""
        if event.source == self.process.myId:
            return

        if event.get_original_sender() != self.process.myId:
            return

        msg_id = event.get_msg_id()

        # Retirer ce message de la liste d'attente
        if msg_id in self.pending_sendto_acks:
            del self.pending_sendto_acks[msg_id]
            self.logger.log(f"Received acknowledgment from P{event.source} for sync message")