"""Launcher du système de processus distribués.

Ce module démarre un ensemble de processus simulés, les laisse
fonctionner pendant un certain temps, puis effectue un arrêt propre
en demandant à chaque processus de s'arrêter et en attendant leur fin.

Fonctions principales:
- launch(nbProcess, runningTime): crée, exécute puis arrête les processus.
"""
from time import sleep
from Process import Process

def launch(nbProcess, runningTime=10):
    """Lance le système avec un nombre donné de processus.

    Args:
        nbProcess: Nombre de processus à créer.
        runningTime: Durée (en secondes) pendant laquelle le système tourne
            avant l'arrêt propre.

    Comportement:
        - Crée tous les processus presque simultanément pour garantir une
          unique phase d'élection du coordinateur.
        - Laisse le système tourner, puis demande un arrêt et attend la
          terminaison de tous les threads.
    """
    processes = []

    # Créer tous les processus simultanément pour une élection unique
    print(f"=== SYSTEM START ===")
    print(f"Creating {nbProcess} processes...")
    arbitrary_names = ["Alice", "Bob", "Charlie"]
    for i in range(nbProcess):
        name = arbitrary_names[i] if i < len(arbitrary_names) else f"Process_{i}"
        processes.append(Process(name, nbProcess))
        sleep(0.1)  # Délai minimal juste pour éviter les erreurs PyEventBus3

    print("All processes created, running system...")
    sleep(runningTime)

    print("=== SYSTEM SHUTDOWN ===")
    for p in processes:
        p.stop()

    for p in processes:
        p.waitStopped()

    print("All processes stopped.")


if __name__ == '__main__':

    #bus = EventBus.getInstance()

    launch(nbProcess=3, runningTime=20)

    #bus.stop()