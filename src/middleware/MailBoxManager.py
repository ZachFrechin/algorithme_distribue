"""Gestion thread-safe des messages utilisateur via une boîte aux lettres."""
from threading import Lock
from Message import Message, BroadcastMessage, DedicatedMessage


class MailBoxManager:
    """Gestionnaire de la boîte aux lettres pour les messages utilisateur."""

    def __init__(self, logical_clock):
        self.logical_clock = logical_clock
        self._mail_box_mutex = Lock()
        self.mail_box = []

    def put_message(self, message: Message):
        """Ajoute un message dans la boîte aux lettres."""
        if not message.USER_MESSAGE:
            raise TypeError("Only user messages can be put in the mail box")
        with self._mail_box_mutex:
            self.mail_box.append(message)
            self.logical_clock.update_on_receive(message.stamp)

    def get_message(self):
        """Récupère le premier message de la boîte aux lettres."""
        with self._mail_box_mutex:
            if len(self.mail_box) == 0:
                return None
            return self.mail_box.pop(0)

    def get_messages(self):
        """Récupère tous les messages de la boîte aux lettres."""
        with self._mail_box_mutex:
            messages = self.mail_box
            self.mail_box = []
            return messages

    def get_message_from(self, source):
        """Récupère le premier message d'une source spécifique."""
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return None
            self.mail_box.remove(messages[0])
            return messages[0]

    def get_messages_from(self, source):
        """Récupère tous les messages d'une source spécifique."""
        with self._mail_box_mutex:
            messages = [message for message in self.mail_box if message.source == source]
            if len(messages) == 0:
                return []
            self.mail_box = [message for message in self.mail_box if message.source != source]
            return messages

    def has_messages(self):
        """Vérifie s'il y a des messages dans la boîte aux lettres."""
        with self._mail_box_mutex:
            return len(self.mail_box) > 0

    def get_messages_type(self, message_type=None):
        """Récupère tous les messages d'un type spécifique."""
        if message_type is None:
            return None

        with self._mail_box_mutex:
            match message_type:
                case "broadcast":
                    messages = [message for message in self.mail_box if isinstance(message, BroadcastMessage)]
                case "dedicated":
                    messages = [message for message in self.mail_box if isinstance(message, DedicatedMessage)]
                case _:
                    return None
            return messages

    def get_message_type(self, message_type=None):
        """Récupère le premier message d'un type spécifique."""
        if message_type is None:
            return None
        messages = self.get_messages_type(message_type)
        if messages is None or len(messages) == 0:
            return None
        return messages[0]

    def count_messages(self):
        """Retourne le nombre de messages dans la boîte aux lettres."""
        with self._mail_box_mutex:
            return len(self.mail_box)

    def peek_message(self):
        """Regarde le premier message sans le retirer."""
        with self._mail_box_mutex:
            if len(self.mail_box) == 0:
                return None
            return self.mail_box[0]

    def clear_mailbox(self):
        """Vide complètement la boîte aux lettres."""
        with self._mail_box_mutex:
            self.mail_box.clear()