"""Définition de la hiérarchie de messages échangés entre processus.

Ce module contient des classes de messages (utilisateur et système)
utilisées par la couche de communication pour le broadcast, le point-à-
point, la synchronisation, la gestion du token et la gestion d'identité.
Chaque message transporte un `payload`, un `stamp` (horloge logique) et
des métadonnées comme `source` et parfois `dest`.
"""
from abc import ABC
from Token import Token

class Message(ABC):
    """Message de base.

    Attributs:
        payload: Contenu porté par le message (str/int/obj selon le type).
        stamp: Valeur d'horloge logique Lamport au moment de l'émission.
        source: Identifiant du processus émetteur (ou temp_id au boot).
    """
    def __init__(self, payload, stamp, source = None):
        self.payload = payload
        self.stamp = stamp
        self.source = source

    @classmethod
    def new_message(cls, payload, stamp, source = None):
        return cls(payload, stamp, source)

    @classmethod
    def from_payload(cls, payload, stamp, source = None):
        return cls(payload, stamp, source)

    @classmethod
    def empty_payload(cls, stamp, source):
        return cls.from_payload(None, stamp, source)

    def send(self, pybus):
        """Publie ce message sur le bus d'événements."""
        pybus.post(self)

    def broadcast(self, pybus):
        """Publie ce message pour tous les abonnés (broadcast)."""
        pybus.post(self)

    def get_payload(self):
        """Retourne le payload du message."""
        return self.payload
    
    def get_stamp(self):
        """Retourne l'horodatage logique (Lamport)."""
        return self.stamp

class UserMessage(Message):
    """Message destiné à la couche applicative (va dans la mailbox)."""
    def __init__(self, payload, stamp, source = None):
        super().__init__(payload, stamp, source)
        self.USER_MESSAGE = True
        self.SYSTEM_MESSAGE = False

class SystemMessage(Message):
    """Message interne au middleware (non livré à la mailbox utilisateur)."""
    def __init__(self, payload, stamp, source = None):
        super().__init__(payload, stamp, source)
        self.SYSTEM_MESSAGE = True
        self.USER_MESSAGE = False

class BroadcastMessage(UserMessage):
    """Message utilisateur envoyé en diffusion à tous les processus."""
    @classmethod
    def new_broadcast_message(cls, payload, stamp, source = None):
        return cls.from_payload(payload, stamp, source)

class DedicatedMessage(UserMessage):
    """Message utilisateur point-à-point avec un destinataire unique."""
    def __init__(self, payload, stamp, dest, source = None):
        super().__init__(payload, stamp, source)
        self.dest = dest    

    @classmethod
    def new_dedicated_message(cls, payload, stamp, dest, source = None):
        return cls(payload, stamp, dest, source)  

    def get_dest(self):
        """Retourne l'identifiant du destinataire."""
        return self.dest

class TokenMessage(SystemMessage):
    """Message système pour transporter le token du ring vers `dest`."""
    def __init__(self, token, stamp, dest, source = None):
        self.token = token
        super().__init__(self.token, stamp, source)
        self.dest = dest

    @classmethod
    def new_token_message(cls, token, stamp, dest, source = None):
        return cls(token, stamp, dest, source)

    def get_dest(self):
        """Retourne l'identifiant du destinataire du token."""
        return self.dest

    def get_token(self):
        """Retourne l'objet `Token` transporté."""
        return self.token

class SyncMessage(UserMessage):
    """Message utilisateur pour protocoles de synchronisation (barrières)."""
    def __init__(self, msg_type, stamp, source = None):
        super().__init__(msg_type, stamp, source)
        self.msg_type = msg_type

    @classmethod
    def new_sync_message(cls, msg_type, stamp, source = None):
        return cls(msg_type, stamp, source)

    def get_type(self):
        """Retourne le type de message de synchronisation."""
        return self.msg_type

class RegistrationMessage(UserMessage):
    """Message utilisateur pour demander un enregistrement/ID (hérité)."""
    def __init__(self, stamp, source = None):
        super().__init__("ID_REQUEST", stamp, source)

    @classmethod
    def new_registration_message(cls, stamp, source = None):
        return cls(stamp, source)

class RegisterMessage(UserMessage):
    """Message utilisateur contenant un ID de processus (hérité)."""
    def __init__(self, process_id, stamp, source = None):
        super().__init__(process_id, stamp, source)
        self.process_id = process_id

    @classmethod
    def new_register_message(cls, process_id, stamp, source = None):
        return cls(process_id, stamp, source)

    def get_process_id(self):
        """Retourne l'identifiant de processus porté."""
        return self.process_id

class RequestMaxIdMessage(UserMessage):
    """Message pour demander qui a l'ID maximum"""
    def __init__(self, stamp, source = None):
        super().__init__("REQUEST_MAX_ID", stamp, source)

    @classmethod
    def new_request_max_id_message(cls, stamp, source = None):
        return cls(stamp, source)

class ResponseMaxIdMessage(UserMessage):
    """Message pour répondre avec son ID maximum"""
    def __init__(self, current_max_id, stamp, source = None):
        super().__init__(current_max_id, stamp, source)
        self.current_max_id = current_max_id

    @classmethod
    def new_response_max_id_message(cls, current_max_id, stamp, source = None):
        return cls(current_max_id, stamp, source)

    def get_max_id(self):
        return self.current_max_id

class BroadcastSyncMessage(UserMessage):
    """Message pour broadcast synchrone avec accusé de réception"""
    def __init__(self, payload, stamp, sender, msg_id, source = None):
        super().__init__(payload, stamp, source)
        self.sender = sender
        self.msg_id = msg_id

    @classmethod
    def new_broadcast_sync_message(cls, payload, stamp, sender, msg_id, source = None):
        return cls(payload, stamp, sender, msg_id, source)

    def get_sender(self):
        return self.sender

    def get_msg_id(self):
        return self.msg_id

class BroadcastSyncAckMessage(SystemMessage):
    """Message d'accusé de réception pour broadcast synchrone"""
    def __init__(self, msg_id, stamp, original_sender, source = None):
        super().__init__(msg_id, stamp, source)
        self.msg_id = msg_id
        self.original_sender = original_sender

    @classmethod
    def new_broadcast_sync_ack_message(cls, msg_id, stamp, original_sender, source = None):
        return cls(msg_id, stamp, original_sender, source)

    def get_msg_id(self):
        return self.msg_id

    def get_original_sender(self):
        return self.original_sender

class SendToSyncMessage(UserMessage):
    """Message pour communication point-à-point synchrone"""
    def __init__(self, payload, stamp, dest, msg_id, source = None):
        super().__init__(payload, stamp, source)
        self.dest = dest
        self.msg_id = msg_id

    @classmethod
    def new_send_to_sync_message(cls, payload, stamp, dest, msg_id, source = None):
        return cls(payload, stamp, dest, msg_id, source)

    def get_dest(self):
        return self.dest

    def get_msg_id(self):
        return self.msg_id

class SendToSyncAckMessage(SystemMessage):
    """Message d'accusé de réception pour communication point-à-point synchrone"""
    def __init__(self, msg_id, stamp, original_sender, source = None):
        super().__init__(msg_id, stamp, source)
        self.msg_id = msg_id
        self.original_sender = original_sender

    @classmethod
    def new_send_to_sync_ack_message(cls, msg_id, stamp, original_sender, source = None):
        return cls(msg_id, stamp, original_sender, source)

    def get_msg_id(self):
        return self.msg_id

    def get_original_sender(self):
        return self.original_sender

class IdRequestMessage(SystemMessage):
    """Message pour demander un ID unique"""
    def __init__(self, stamp, source = None):
        super().__init__("ID_REQUEST", stamp, source)

    @classmethod
    def new_id_request_message(cls, stamp, source = None):
        return cls(stamp, source)

class IdAssignmentMessage(SystemMessage):
    """Message pour assigner un ID à un processus"""
    def __init__(self, assigned_id, stamp, dest, source = None):
        super().__init__(assigned_id, stamp, source)
        self.assigned_id = assigned_id
        self.dest = dest

    @classmethod
    def new_id_assignment_message(cls, dest, assigned_id, stamp, source = None):
        return cls(assigned_id, stamp, dest, source)

    def get_assigned_id(self):
        return self.assigned_id

    def get_dest(self):
        return self.dest

class ProcessCountUpdateMessage(SystemMessage):
    """Message pour mettre à jour le nombre total de processus"""
    def __init__(self, new_count, stamp, source = None):
        super().__init__(f"PROCESS_COUNT_UPDATE:{new_count}", stamp, source)
        self.new_count = new_count

    @classmethod
    def new_process_count_update_message(cls, new_count, stamp, source = None):
        return cls(new_count, stamp, source)

class HeartbeatMessage(SystemMessage):
    """Message heartbeat pour prouver qu'un processus est vivant"""
    def __init__(self, stamp, source = None):
        super().__init__("HEARTBEAT", stamp, source)

    @classmethod
    def new_heartbeat_message(cls, stamp, source = None):
        return cls(stamp, source)


class IdReassignmentMessage(SystemMessage):
    """Message pour réassigner un nouvel ID à un processus après panne."""

    def __init__(self, old_id, new_id, stamp, source=None):
        self.old_id = old_id
        self.new_id = new_id
        super().__init__(f"ID_REASSIGN:{old_id}:{new_id}", stamp, source)

    @classmethod
    def new_id_reassignment_message(cls, old_id, new_id, stamp, source=None):
        return cls(old_id, new_id, stamp, source)

