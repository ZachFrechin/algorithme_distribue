class Colors:
    """
    Codes de couleurs ANSI pour l'affichage terminal.
    """

    # Couleurs de base
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'

    # Styles
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    # Reset
    RESET = '\033[0m'

    @staticmethod
    def blue_id(text):
        """Affiche un ID temporaire en bleu."""
        return f"{Colors.BLUE}{text}{Colors.RESET}"

    @staticmethod
    def pink_id(text):
        """Affiche un ID externe en rose (magenta)."""
        return f"{Colors.MAGENTA}{text}{Colors.RESET}"

    @staticmethod
    def green_action(text):
        """Affiche une action importante en vert."""
        return f"{Colors.GREEN}{text}{Colors.RESET}"

    @staticmethod
    def yellow_warning(text):
        """Affiche un avertissement en jaune."""
        return f"{Colors.YELLOW}{text}{Colors.RESET}"

    @staticmethod
    def red_error(text):
        """Affiche une erreur en rouge."""
        return f"{Colors.RED}{text}{Colors.RESET}"

    @staticmethod
    def cyan_system(text):
        """Affiche un message système en cyan."""
        return f"{Colors.CYAN}{text}{Colors.RESET}"

    @staticmethod
    def bold_text(text):
        """Affiche un texte en gras."""
        return f"{Colors.BOLD}{text}{Colors.RESET}"