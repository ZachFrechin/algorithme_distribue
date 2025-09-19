"""Objet simple représentant le jeton pour l'algorithme d'anneau à jeton."""
class Token:
    """Représente un token dans l'algorithme d'anneau à jeton"""
    
    def __init__(self, data="TOKEN"):
        self.data = data
    
    def __str__(self):
        return self.data
    
    def __repr__(self):
        return f"Token('{self.data}')"