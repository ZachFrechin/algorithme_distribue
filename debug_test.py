#!/usr/bin/env python3

"""
Debug test pour identifier le problème d'attribution d'ID
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from Process import Process
from time import sleep
import threading

def test_minimal():
    print("=== Debug Test: Attribution d'ID seulement ===\n")

    def timeout_stop():
        sleep(30)  # Timeout après 30 secondes
        print("TIMEOUT - Arrêt forcé")
        os._exit(1)

    # Thread de timeout pour éviter les boucles infinies
    threading.Thread(target=timeout_stop, daemon=True).start()

    # Créer 2 processus pour tester l'attribution d'ID
    print("Création des processus...")
    processes = []
    for i in range(2):
        processes.append(Process(f"P{i}", 2))

    # Attendre un peu
    sleep(10)

    for i, process in enumerate(processes):
        print(f"Processus {i}: ID assigné: {process.myId}, ID assigned flag: {process.id_assigned}, Temp ID: {process.temp_id}")

    # Arrêter les processus
    for process in processes:
        process.stop()

    for process in processes:
        process.waitStopped()

    print("Test terminé.")

if __name__ == "__main__":
    test_minimal()