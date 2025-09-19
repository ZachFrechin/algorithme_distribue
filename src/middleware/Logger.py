"""Logger de processus avec couleurs et filtrage des messages répétitifs."""
import re
from middleware.Colors import Colors

class ProcessLogger:
    """
    Système de logging standardisé pour les processus avec couleurs et filtrage.
    Format : [ID - NAME - CLOCK] : MESSAGE
    """

    def __init__(self, process, clock):
        self.process = process
        self.clock = clock
        self.last_messages = []  # Pour éviter les doublons
        self.max_history = 5     # Garder les 5 derniers messages

    def log(self, message, coordinator_only=False, force=False):
        """
        Affiche un message avec le format standardisé et des couleurs.

        Args:
            message: Le message à afficher
            coordinator_only: Si True, n'affiche que si le processus est coordinateur
            force: Si True, ignore le filtrage des doublons
        """
        if coordinator_only and not self.process.is_coordinator:
            return

        # Filtrer les doublons (sauf si force=True)
        if not force and self._is_duplicate(message):
            return

        # Formater le message avec des couleurs
        formatted_message = self._format_message_with_colors(message)

        # Créer le préfix avec couleurs
        if self.process.id_assigned and self.process.myId is not None:
            prefix = f"[{self.process.myId} - {self.process.myProcessName} - {self.clock.get_time()}]"
        else:
            # ID temporaire en bleu
            temp_id_colored = Colors.blue_id(str(self.process.temp_id))
            prefix = f"[{temp_id_colored} - {self.process.myProcessName} - {self.clock.get_time()}] (non registered)"

        # Afficher le message
        print(f"{prefix} : {formatted_message}")

        # Ajouter à l'historique
        self._add_to_history(message)

    def _is_duplicate(self, message):
        """Vérifie si le message est un doublon récent."""
        # Ne jamais filtrer les messages importants
        important_keywords = [
            "token", "Token", "TOKEN",
            "critical section", "CRITICAL SECTION",
            "DEBUG:"
        ]

        for keyword in important_keywords:
            if keyword in message:
                return False

        # Messages de synchronisation répétitifs - mais pas tous !
        sync_patterns = [
            r"Auto-joining sync barrier \(triggered by P\d+\)",
            r"Still waiting for \d+ acknowledgments",
            r"Loop: \d+"
        ]

        for pattern in sync_patterns:
            if re.search(pattern, message):
                # Compter les occurrences récentes
                recent_count = self.last_messages.count(message)
                if recent_count >= 2:  # Permettre 2 fois avant de filtrer
                    return True

        return False

    def _format_message_with_colors(self, message):
        """Ajoute des couleurs au message selon le contexte."""
        # Colorier les IDs d'autres processus en rose
        message = re.sub(r'\bP(\d+)\b', lambda m: f"P{Colors.pink_id(m.group(1))}", message)

        # Colorier les actions importantes
        if "Election successful" in message:
            message = Colors.green_action(message)
        elif "Heartbeat system started" in message:
            message = Colors.cyan_system(message)
        elif "CRITICAL SECTION" in message or "critical section" in message:
            message = Colors.yellow_warning(message)
        elif any(token_word in message for token_word in ["token", "Token", "TOKEN"]):
            message = Colors.yellow_warning(message)
        elif "Broadcasting sync" in message:
            message = Colors.green_action(message)
        elif "All processes synchronized" in message:
            message = Colors.green_action(message)
        elif "detected as failed" in message:
            message = Colors.red_error(message)
        elif "Received assigned ID" in message:
            message = Colors.green_action(message)

        return message

    def _add_to_history(self, message):
        """Ajoute le message à l'historique en gardant seulement les derniers."""
        self.last_messages.append(message)
        if len(self.last_messages) > self.max_history:
            self.last_messages.pop(0)

    def log_section(self, title):
        """Affiche un titre de section avec des couleurs."""
        separator = "=" * 50
        print(f"\n{Colors.cyan_system(separator)}")
        print(f"{Colors.bold_text(title.center(50))}")
        print(f"{Colors.cyan_system(separator)}")

    def log_important(self, message):
        """Affiche un message important qui ne peut pas être filtré."""
        self.log(message, force=True)