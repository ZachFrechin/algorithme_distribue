#!/usr/bin/env python3

"""
Test simple pour Phase 3 - Communications synchrones et attribution automatique d'ID
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from Process import Process
from time import sleep

def test_phase3():
    print("=== Test Phase 3: Attribution automatique d'ID et communications synchrones ===\n")

    # Créer 3 processus
    processes = []
    for i in range(3):
        processes.append(Process(f"P{i}", 3))

    # Attendre que les IDs soient assignés
    print("Attente de l'attribution des IDs...")
    sleep(5)

    # Vérifier les IDs assignés
    for process in processes:
        if process.id_assigned:
            print(f"Processus {process.myProcessName} a reçu l'ID: {process.myId}")
        else:
            print(f"ERREUR: Processus {process.myProcessName} n'a pas d'ID assigné")

    # Laisser tourner pour voir les communications
    print("\nLancement des tests de communication...")
    sleep(15)

    # Arrêter tous les processus
    print("\nArrêt des processus...")
    for process in processes:
        process.stop()

    for process in processes:
        process.waitStopped()

    print("Test terminé.")

if __name__ == "__main__":
    test_phase3()