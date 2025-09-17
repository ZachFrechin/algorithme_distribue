# Système de Processus Distribués

Ce projet implémente un système de processus distribués avec différents mécanismes de communication et de synchronisation, utilisant le pattern EventBus pour la communication inter-processus.

## Architecture

### Structure des fichiers

```
app/
├── Process.py      # Classe principale des processus
├── Message.py      # Hiérarchie des messages
├── Token.py        # Implémentation du jeton
├── Launcher.py     # Point d'entrée pour lancer les processus
└── README.md       # Cette documentation
```

## Types de Messages

### 1. BroadcastMessage
**Objectif** : Diffusion d'un message à tous les processus

```python
self.broadcast("hello")  # Envoie "hello" à tous les processus
```

**Fonctionnement** :
- Le message est diffusé via l'EventBus à tous les processus abonnés
- Chaque processus reçoit le message via `on_broadcast()`
- **Mise à jour d'horloge** : `clock = max(clock_local, timestamp_message) + 1`
- Le processus émetteur n'écoute pas son propre message

### 2. DedicatedMessage
**Objectif** : Envoi ciblé vers un processus spécifique

```python
self.send_to("hello", "P2")  # Envoie "hello" au processus P2
```

**Fonctionnement** :
- Message avec destination spécifique (`dest`)
- Seul le processus destinataire traite le message via `on_receive()`
- **Mise à jour d'horloge** : `clock = max(clock_local, timestamp_message) + 1`

### 3. TokenMessage
**Objectif** : Circulation du jeton pour l'exclusion mutuelle

```python
self.send_token()  # Envoie le jeton au processus suivant
```

**Fonctionnement** :
- Hérite de `DedicatedMessage` avec un jeton en payload
- Circulation en anneau : processus `i` → processus `(i+1) % n`
- Le processus perd le jeton après l'envoi (`self.token = None`)

## Système de Jetons (Token Ring)

### Principe
Implémentation de l'algorithme d'**anneau à jeton** pour l'exclusion mutuelle distribuée.

### Mécanisme

#### États des processus :
- **`SC = 0`** : Pas de demande de section critique
- **`SC = 1`** : Demande en cours
- **`token != None`** : Possession du jeton

#### Algorithme :

1. **Demande d'accès** (`request()`) :
   ```python
   self.SC = 1  # Signal de demande
   while self.token == None and self.SC == 1:  # Attente active
       pass
   ```

2. **Réception du jeton** (`on_token()`) :
   - Si `SC = 0` → Transfer immédiat du jeton
   - Si `SC = 1` → Garde le jeton pour la section critique

3. **Libération** (`release()`) :
   ```python
   self.SC = 0          # Fin de demande
   self.send_token()    # Transfer du jeton
   ```

### Topologie
**Anneau logique** : P0 → P1 → P2 → ... → Pn-1 → P0

## Synchronisation Distribuée

### Principe
Implémentation d'une **barrière de synchronisation centralisée** où tous les processus doivent appeler `synchronize()` avant de continuer.

### Protocole

#### Architecture centralisée :
- **Coordinateur** : Processus 0
- **Participants** : Tous les autres processus

#### Algorithme :

1. **Initialisation** :
   ```python
   self.sync_barrier = True  # Activation de la barrière
   ```

2. **Phase READY** :
   - Tous les processus (y compris le coordinateur) envoient `SyncMessage("READY")`
   - Le coordinateur compte les messages READY reçus

3. **Phase RELEASE** :
   ```python
   if self.sync_ready_count == self.npProcess:  # Tous prêts
       SyncMessage("SYNC_RELEASE").send()       # Signal de déblocage
   ```

4. **Déblocage** :
   - Tous les processus reçoivent `SYNC_RELEASE`
   - `self.sync_barrier = False` → Sortie de la boucle d'attente

### Point critique
⚠️ **Le coordinateur ne doit PAS se débloquer avant d'envoyer SYNC_RELEASE**, sinon les autres processus restent bloqués.

## Horloge Logique de Lamport

### Principe
Chaque processus maintient une horloge logique (`self.clock`) pour ordonner les événements distribués.

### Règles de mise à jour :

1. **Événement local** :
   ```python
   self.clock += 1  # Avant envoi de message
   ```

2. **Réception de message** :
   ```python
   self.clock = max(self.clock, message.timestamp) + 1
   ```

### Application
- Estampillage de tous les messages avec `process.clock`
- Synchronisation des horloges lors des communications
- Détection de la causalité entre événements

## EventBus et Threading

### PyEventBus3
- **Communication asynchrone** entre processus via publish/subscribe
- **Mode PARALLEL** : Traitement concurrent des messages
- **Abonnements typés** : Chaque type de message a son handler

### Threading
- Chaque processus est un `Thread` indépendant
- **Boucle principale** dans `run()` avec états et actions cycliques
- **Handlers asynchrones** pour les messages entrants

## Utilisation

### Lancement
```bash
python Launcher.py
```

### Configuration
```python
launch(nbProcess=3, runningTime=5)  # 3 processus, 5 secondes
```

### Exemple de sortie
```
MainThread-P0 Loop: 0 clock: 0
MainThread-P1 Loop: 0 clock: 0
MainThread-P2 Loop: 0 clock: 0
MainThread-P0 calling synchronize() at loop 1
MainThread-P1 calling synchronize() at loop 1
MainThread-P2 calling synchronize() at loop 1
MainThread-P0 received READY from process 0 (1/3)
MainThread-P0 received READY from process 1 (2/3)
MainThread-P0 received READY from process 2 (3/3)
MainThread-P0 all processes ready, releasing synchronization barrier
MainThread-P0 received SYNC_RELEASE, proceeding
MainThread-P1 received SYNC_RELEASE, proceeding
MainThread-P2 received SYNC_RELEASE, proceeding
MainThread-P0 passed synchronization barrier, clock: 4
MainThread-P1 passed synchronization barrier, clock: 4
MainThread-P2 passed synchronization barrier, clock: 4
```

## Points d'Attention

### Problèmes courants

1. **Blocage de synchronisation** :
   - Le coordinateur doit traiter son propre SYNC_RELEASE
   - Ne pas mettre `sync_barrier = False` prématurément

2. **Fuite de jetons** :
   - Vérifier que `send_token()` est appelé dans `release()`
   - Gérer les cas où un processus meurt avec le jeton

3. **Conditions de course** :
   - Utiliser des verrous si nécessaire pour les variables partagées
   - Attention aux accès concurrents via EventBus

### Debugging
- Activer les logs avec `print()` pour tracer les messages
- Vérifier les horloges logiques pour la cohérence temporelle
- Surveiller les compteurs de synchronisation

---

*Ce système illustre les concepts fondamentaux des systèmes distribués : communication, synchronisation, exclusion mutuelle et cohérence temporelle.*