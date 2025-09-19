# Documentation Technique Complète - Module Com
## Middleware de Communication pour Systèmes Distribués

### Table des matières
1. [Introduction](#introduction)
2. [Architecture Générale](#architecture-générale)
3. [Classe Com - Interface Principale](#classe-com---interface-principale)
4. [Gestion de l'Horloge Logique](#gestion-de-lhorloge-logique)
5. [Système de Messages Asynchrones](#système-de-messages-asynchrones)
6. [Système de Messages Synchrones](#système-de-messages-synchrones)
7. [Token Ring et Section Critique](#token-ring-et-section-critique)
8. [Barrières de Synchronisation](#barrières-de-synchronisation)
9. [Système d'Élection et Attribution d'IDs](#système-délection-et-attribution-dids)
10. [Détection de Pannes et Heartbeat](#détection-de-pannes-et-heartbeat)
11. [Gestionnaires d'Événements](#gestionnaires-dévénements)
12. [Cycle de Vie et États](#cycle-de-vie-et-états)

---

## Introduction

Le module Com constitue le cœur du middleware de communication pour systèmes distribués. Il fournit une abstraction complète pour la communication inter-processus, gérant automatiquement l'attribution d'identifiants uniques, les élections de leader, la synchronisation distribuée, et offre des primitives de communication synchrones et asynchrones.

### Objectifs principaux
- Fournir une interface unifiée pour les communications distribuées
- Gérer automatiquement l'attribution d'identifiants uniques
- Implémenter des mécanismes de synchronisation robustes
- Détecter et gérer les pannes de processus
- Garantir l'exclusion mutuelle via token ring

---

## Architecture Générale

Le module Com s'articule autour de plusieurs composants modulaires interconnectés:

```
Com (Classe principale)
├── LogicalClock (Horloge de Lamport)
├── ProcessLogger (Système de logs)
├── ElectionManager (Gestion des élections)
├── MailBoxManager (Messages asynchrones)
├── SyncCommunication (Messages synchrones)
├── HeartbeatManager (Détection de pannes)
└── Token Ring (Section critique)
```

### Principe de fonctionnement

Chaque processus dans le système distribué possède une instance de la classe Com qui:
1. S'enregistre sur le bus d'événements PyEventBus3
2. Participe automatiquement aux élections pour l'attribution d'ID
3. Maintient une horloge logique cohérente
4. Gère une boîte aux lettres pour les messages asynchrones
5. Fournit des primitives de synchronisation

---

## Classe Com - Interface Principale

### Constructeur et Initialisation

```python
def __init__(self, process=None)
```

**Description détaillée:**
Le constructeur initialise tous les composants nécessaires au fonctionnement du middleware. L'initialisation suit un ordre spécifique pour garantir les dépendances entre composants.

**Processus d'initialisation:**
1. Création de l'horloge logique (LogicalClock)
2. Initialisation du logger avec référence à l'horloge
3. Création de l'ElectionManager pour gérer les élections
4. Initialisation du SyncCommunication pour les messages synchrones
5. Création du MailBoxManager pour les messages asynchrones
6. Initialisation du HeartbeatManager pour la détection de pannes
7. Configuration du token ring (token=None, mutex, SC=0)
8. Initialisation des compteurs de synchronisation
9. Enregistrement sur PyEventBus

### Attributs de la classe

#### Attributs de base
- **process** (Process): Référence vers le processus parent contenant l'ID et l'état
- **logical_clock** (LogicalClock): Instance gérant l'horloge logique de Lamport
- **logger** (ProcessLogger): Système de journalisation avec formatage standardisé

#### Gestionnaires spécialisés
- **election_manager** (ElectionManager): Gère les élections de leader et l'attribution d'IDs
- **sync_comm** (SyncCommunication): Gère les communications synchrones bloquantes
- **mailbox_manager** (MailBoxManager): Gère la boîte aux lettres thread-safe
- **heartbeat_manager** (HeartbeatManager): Gère la détection de pannes

#### Token Ring et Section Critique
- **token** (Token|None): Token actuellement détenu par ce processus
- **token_mutex** (threading.Lock): Verrou pour l'accès thread-safe au token
- **SC** (int): État de la section critique
  - 0: Aucune demande en cours
  - 1: Demande d'accès émise, en attente du token
  - 2: Token possédé, accès à la section critique autorisé

#### Synchronisation distribuée
- **nbProcess** (int): Nombre total de processus actifs dans le système
- **sync_ready_count** (int): Compteur de processus prêts pour la synchronisation
- **sync_barrier** (bool): Indicateur de synchronisation en cours
- **sync_generation** (int): Numéro de génération de la barrière actuelle
- **ready_sent_for_generation** (set): Ensemble des générations pour lesquelles READY a été envoyé

---

## Gestion de l'Horloge Logique

### Algorithme de Lamport

L'horloge logique implémente l'algorithme de Lamport pour maintenir un ordre causal entre les événements distribués.

#### inc_clock(self) -> int

**Fonctionnement détaillé:**
1. Incrémente la valeur locale de l'horloge de 1
2. Retourne la nouvelle valeur
3. Garantit que chaque événement local a un timestamp unique et croissant

**Cas d'usage:**
- Automatiquement appelé lors de l'envoi de tout message
- Peut être appelé manuellement pour marquer des événements locaux importants

#### get_clock(self) -> int

**Fonctionnement:**
Retourne la valeur actuelle de l'horloge sans la modifier. Thread-safe grâce à l'implémentation atomique.

#### _update_clock_on_receive(self, clock) -> int

**Algorithme détaillé:**
```
1. Récupère l'horloge locale actuelle (local_clock)
2. Récupère le timestamp du message reçu (received_clock)
3. Calcule: new_clock = max(local_clock, received_clock) + 1
4. Met à jour l'horloge locale avec new_clock
5. Retourne new_clock
```

**Garanties:**
- L'horloge locale est toujours supérieure au maximum entre elle-même et le timestamp reçu
- Préserve l'ordre causal des événements
- Thread-safe via implémentation atomique

---

## Système de Messages Asynchrones

### Vue d'ensemble

Le système de messages asynchrones utilise une architecture publish-subscribe via PyEventBus3. Les messages sont déposés dans une boîte aux lettres et peuvent être récupérés ultérieurement.

### Envoi de messages

#### broadcast(self, payload)

**Mécanisme détaillé:**
1. Vérifie que le processus est initialisé (process != None)
2. Incrémente l'horloge logique locale
3. Crée un BroadcastMessage avec:
   - payload: contenu du message
   - timestamp: valeur actuelle de l'horloge
   - source: ID du processus émetteur
4. Publie le message sur PyEventBus
5. Retourne immédiatement (non-bloquant)

**Propagation:**
- Le message est reçu par TOUS les processus enregistrés
- Chaque processus filtre ses propres messages
- Les messages système sont traités séparément
- Les messages utilisateur vont dans la mailbox

#### send_to(self, payload, dest)

**Mécanisme détaillé:**
1. Vérifie que le processus est initialisé
2. Incrémente l'horloge logique
3. Crée un DedicatedMessage avec:
   - payload: contenu
   - timestamp: horloge actuelle
   - dest: ID du destinataire
   - source: ID de l'émetteur
4. Publie sur PyEventBus
5. Retourne immédiatement

**Filtrage à la réception:**
- Seul le processus avec ID == dest traite le message
- Les autres processus ignorent automatiquement

### Gestion de la Mailbox

La mailbox est une structure FIFO thread-safe pour stocker les messages asynchrones reçus.

#### put_message(self, message: Message)

**Mécanisme interne:**
1. Vérifie que message.USER_MESSAGE == True
2. Acquiert le mutex de la mailbox
3. Ajoute le message à la fin de la liste
4. Met à jour l'horloge logique avec le timestamp du message
5. Libère le mutex

**Exceptions:**
- NotUserMessageException: si tentative d'insertion d'un message système

#### get_message(self) -> Message|None

**Fonctionnement FIFO:**
1. Acquiert le mutex
2. Vérifie si la mailbox est vide
3. Si non vide: retire et retourne le premier message (index 0)
4. Si vide: retourne None
5. Libère le mutex

#### get_messages(self) -> List[Message]

**Récupération complète:**
1. Acquiert le mutex
2. Copie la référence de la liste actuelle
3. Réinitialise la mailbox avec une liste vide
4. Libère le mutex
5. Retourne l'ancienne liste

**Note:** Cette méthode vide complètement la mailbox

#### get_message_from(self, source) -> Message|None

**Filtrage par source:**
1. Acquiert le mutex
2. Parcourt la mailbox pour trouver le premier message avec source == source
3. Si trouvé: retire le message de la mailbox et le retourne
4. Si non trouvé: retourne None
5. Libère le mutex

#### get_messages_from(self, source) -> List[Message]

**Récupération multiple par source:**
1. Acquiert le mutex
2. Filtre tous les messages avec source == source
3. Retire ces messages de la mailbox
4. Libère le mutex
5. Retourne la liste filtrée

#### get_message_type(self, message_type) -> Message|None

**Types supportés:**
- "broadcast": messages de type BroadcastMessage
- "dedicated": messages de type DedicatedMessage

**Mécanisme:**
1. Utilise isinstance() pour vérifier le type
2. Retourne le premier message du type demandé
3. None si aucun message du type

#### has_messages(self) -> bool

**Vérification non-bloquante:**
1. Acquiert le mutex
2. Vérifie len(mail_box) > 0
3. Libère le mutex
4. Retourne le résultat booléen

---

## Système de Messages Synchrones

### Principe général

Les communications synchrones bloquent l'émetteur jusqu'à réception d'un accusé de réception. Elles garantissent que le message a été reçu et traité.

### broadcastSync(self, payload, sender_id)

**Protocole détaillé:**
1. Crée un BroadcastSyncMessage avec un ID unique
2. Publie le message sur PyEventBus
3. Initialise un compteur d'ACKs attendus (nbProcess - 1)
4. Entre dans une boucle d'attente active
5. Pour chaque ACK reçu, décrémente le compteur
6. Sort de la boucle quand compteur == 0
7. Retourne le contrôle à l'appelant

**Gestion des timeouts:**
- Timeout par défaut: 5 secondes
- Si timeout atteint: exception ou retour d'erreur
- Possibilité de configurer le timeout

### sendToSync(self, payload, dest)

**Protocole point à point:**
1. Crée un SendToSyncMessage avec ID unique
2. Envoie au processus destinataire
3. Entre en attente active d'un ACK spécifique
4. Le destinataire:
   - Reçoit le message
   - Traite le contenu
   - Envoie un SendToSyncAckMessage
5. À réception de l'ACK, retourne

**Garanties:**
- Message effectivement reçu et traité
- Ordre FIFO préservé avec le même destinataire

### recevFromSync(self, source) -> str

**Mécanisme de réception bloquante:**
1. Entre dans une boucle d'attente
2. Vérifie périodiquement l'arrivée d'un SendToSyncMessage
3. Filtre par source == source
4. Quand message trouvé:
   - Extrait le payload
   - Envoie automatiquement l'ACK
   - Retourne le payload
5. Continue l'attente sinon

**Symétrie avec sendToSync:**
- Doit être appelé en miroir d'un sendToSync
- Bloque indéfiniment jusqu'à réception

---

## Token Ring et Section Critique

### Architecture du Token Ring

Le système implémente un algorithme de token ring pour garantir l'exclusion mutuelle. Un token unique circule entre les processus selon une topologie en anneau logique.

### Topologie de l'anneau

**Formation de l'anneau:**
```
P0 -> P1 -> P2 -> ... -> Pn-1 -> P0
```

Chaque processus connaît son successeur via la méthode next_node():
```python
next = (myId + 1) % nbProcess
```

### États de la Section Critique (SC)

**État 0 - Libre:**
- Aucune demande d'accès en cours
- Si token reçu, le transmet immédiatement au suivant
- Délai d'1 seconde avant transmission pour visualisation

**État 1 - Demande émise:**
- Le processus a appelé request_SC()
- Attend la réception du token
- Quand token reçu: passe à l'état 2

**État 2 - En section critique:**
- Possède le token
- Accès exclusif aux ressources partagées
- Reste dans cet état jusqu'à appel de release_SC()

### request_SC(self)

**Algorithme détaillé:**
1. Met SC = 1 (marque la demande)
2. Log l'intention d'accès
3. Entre dans une boucle d'attente active:
   ```python
   while self.SC != 2:
       sleep(0.01)
   ```
4. La boucle se termine quand on_token() met SC = 2
5. Retourne le contrôle (accès accordé)

**Propriétés:**
- Bloquant jusqu'à obtention du token
- Garantit l'exclusion mutuelle
- Équitable: ordre FIFO des demandes

### on_token(self, event: TokenMessage)

**Traitement de réception du token:**
```python
1. Délai d'1 seconde (visualisation)
2. Vérifications de sécurité:
   - process.alive == True
   - event.source != myId (pas mon propre token)
   - event.dest == myId (destiné à moi)
3. Si SC == 1 (demande en attente):
   - Log "Keeping token for critical section"
   - Met SC = 2 (accès accordé)
   - Attend jusqu'à SC == 0 (release_SC appelé)
4. Si SC == 0 (pas de demande):
   - Transfère immédiatement au suivant
5. Appelle send_token() pour la transmission
```

### release_SC(self)

**Mécanisme de libération:**
1. Log "Exiting critical section"
2. Met SC = 0
3. Le handler on_token() détecte SC == 0
4. Déclenche automatiquement send_token()
5. Token transmis au processus suivant

### send_token(self, token=None)

**Protocole de transmission:**
1. Détermine le prochain processus:
   ```python
   next_process = self.process.next_node()
   ```
2. Si token fourni: transmet ce token
3. Si token=None: transmet self.token
4. Crée un TokenMessage:
   - token: objet Token
   - timestamp: horloge actuelle
   - dest: next_process
   - source: myId
5. Publie sur PyEventBus
6. Si c'était notre token: met self.token = None
7. Délai de 0.5s pour visualisation

**Gestion des pannes:**
Si le processus suivant est en panne:
- Le HeartbeatManager met à jour la liste des processus actifs
- next_node() saute automatiquement les processus défaillants
- Le token continue à circuler

---

## Barrières de Synchronisation

### Principe général

Les barrières de synchronisation permettent à tous les processus de se synchroniser à un point donné. Aucun processus ne peut continuer tant que tous n'ont pas atteint la barrière.

### Mécanisme des générations

**Problème résolu:**
Les générations évitent la confusion entre différentes barrières consécutives ou simultanées.

**Fonctionnement:**
- Chaque barrière a un numéro de génération unique
- Les messages READY incluent la génération
- Les messages d'anciennes générations sont ignorés
- Évite les boucles infinies de synchronisation

### synchronize(self)

**Algorithme complet:**
```python
1. Vérification préliminaire:
   - process.id_assigned == True
   - myId != None

2. Nouvelle génération:
   sync_generation += 1

3. Vérification anti-doublon:
   if sync_generation in ready_sent_for_generation:
       return  # Déjà participé

4. Initialisation:
   sync_barrier = True
   sync_ready_count = 0

5. Envoi du signal READY:
   message = "READY:" + str(sync_generation)
   ready_sent_for_generation.add(sync_generation)
   send_sync_message(message)

6. Attente active:
   while sync_barrier:
       sleep(0.01)

7. Sortie:
   log("Passed synchronization barrier")
```

### Protocole de synchronisation distribué

**Côté processus ordinaire:**
1. Appelle synchronize() ou détecte un READY entrant
2. Envoie son propre READY avec la génération
3. Attend la réception de SYNC_RELEASE
4. Continue l'exécution

**Côté coordinateur (traitement des READY):**
1. Reçoit les messages READY
2. Incrémente sync_ready_count pour chaque READY
3. Compare avec le nombre de processus actifs:
   ```python
   expected = len(heartbeat_manager.get_active_processes())
   ```
4. Si sync_ready_count >= expected:
   - Envoie SYNC_RELEASE à tous
   - Réinitialise les compteurs

### Auto-jointure à une barrière

**Mécanisme d'auto-détection:**
Quand un processus reçoit un READY d'un autre processus:
1. Vérifie s'il n'est pas déjà en synchronisation
2. Vérifie s'il n'a pas déjà envoyé READY pour cette génération
3. Si non: rejoint automatiquement la barrière
4. Met à jour sa génération locale
5. Envoie son propre READY

**Avantages:**
- Pas besoin que tous appellent synchronize() simultanément
- Un seul processus peut initier la synchronisation
- Robuste face aux variations de timing

### Gestion de la mémoire des générations

**Nettoyage périodique:**
```python
if len(ready_sent_for_generation) > 5:
    sorted_gens = sorted(ready_sent_for_generation)
    old_gens = sorted_gens[:-5]  # Garde les 5 dernières
    for gen in old_gens:
        ready_sent_for_generation.discard(gen)
```

**Objectif:**
- Éviter l'accumulation infinie en mémoire
- Garder suffisamment d'historique pour éviter les doublons

---

## Système d'Élection et Attribution d'IDs

### Vue d'ensemble

Le système d'élection permet l'attribution automatique et décentralisée d'identifiants uniques à chaque processus. Il utilise un algorithme d'élection par comparaison de nombres aléatoires.

### request_dynamic_id(self)

**Algorithme en phases:**

**Phase 1 - Détection de coordinateur existant:**
```python
1. Envoie PING_COORDINATOR en broadcast
2. Attend 1 seconde les réponses PONG_COORDINATOR
3. Si réponse(s) reçue(s):
   - Un coordinateur existe
   - Demande un ID via IdRequestMessage
   - Attend l'attribution
4. Si aucune réponse:
   - Passe à la phase 2
```

**Phase 2 - Vérification d'élection en cours:**
```python
1. Vérifie received_election_numbers
2. Si non vide:
   - Une élection est en cours
   - Rejoint l'élection existante
3. Si vide:
   - Passe à la phase 3
```

**Phase 3 - Nouvelle élection:**
```python
1. Tire un nombre aléatoire (1 à 1,000,000)
2. Diffuse ELECTION_NUMBER:temp_id:number
3. Attend 1.5 secondes pour collecter les nombres
4. Détermine le gagnant
```

### Algorithme d'élection

**Tirage et diffusion:**
```python
my_election_number = random.randint(1, 1000000)
message = f"ELECTION_NUMBER:{temp_id}:{my_election_number}"
broadcast(message)
```

**Collecte des nombres:**
- Stockage dans received_election_numbers[temp_id] = number
- Durée de collecte: 1.5 secondes
- Traitement asynchrone via handlers

**Détermination du gagnant:**
```python
1. Initialisation:
   max_number = my_election_number
   winner_temp_id = my_temp_id

2. Pour chaque nombre reçu:
   if other_number > max_number:
       max_number = other_number
       winner_temp_id = other_temp_id
   elif other_number == max_number:
       if other_temp_id < winner_temp_id:
           winner_temp_id = other_temp_id  # Départage

3. Vérification des égalités:
   if égalité détectée:
       sleep(0.5)  # Anti-synchronisation
       Relancer l'élection

4. Résultat:
   if winner_temp_id == my_temp_id:
       Devenir coordinateur
   else:
       Attendre attribution d'ID
```

### Devenir coordinateur

**Actions du gagnant:**
```python
1. Attribution de l'ID 0:
   myId = 0
   is_coordinator = True
   id_assigned = True

2. Initialisation:
   next_id_to_assign = 1
   name = f"MainThread-P0"

3. Démarrage du heartbeat:
   start_heartbeat()

4. Attribution des IDs aux participants:
   for participant in sorted(received_election_numbers.keys()):
       send IdAssignmentMessage(participant, next_id_to_assign)
       next_id_to_assign += 1

5. Diffusion du nombre de processus:
   broadcast ProcessCountUpdateMessage(next_id_to_assign)
```

### Réception d'un ID

**Traitement par handle_id_assignment():**
```python
1. Vérification:
   if event.dest != my_temp_id:
       return  # Pas pour moi

2. Si déjà assigné:
   if id_assigned:
       return  # Ignorer

3. Attribution:
   myId = event.assigned_id
   id_assigned = True
   name = f"MainThread-P{myId}"

4. Démarrage:
   start_heartbeat()
   election_in_progress = False
```

### Gestion des processus tardifs

**Processus arrivant après l'élection:**
1. Envoie PING_COORDINATOR
2. Reçoit PONG_COORDINATOR du coordinateur
3. Envoie IdRequestMessage
4. Le coordinateur:
   - Assigne le prochain ID disponible
   - Met à jour le compteur global
   - Diffuse la mise à jour du nombre de processus
5. Le nouveau processus démarre son heartbeat

---

## Détection de Pannes et Heartbeat

### Architecture du système de heartbeat

Le système de heartbeat permet la détection automatique des pannes de processus et la réorganisation dynamique du système.

### Mécanisme de heartbeat

**Envoi périodique:**
```python
Toutes les 2 secondes:
1. Incrémente l'horloge logique
2. Crée HeartbeatMessage:
   - timestamp: horloge actuelle
   - source: myId
3. Diffuse en broadcast
4. Sleep(2)
```

**Réception et traitement:**
```python
on_heartbeat(event):
1. if event.source == myId:
       return  # Ignore mon propre heartbeat

2. Met à jour last_heartbeat[source] = current_time()
3. Ajoute source à active_processes
```

### Détection de pannes

**Surveillance par le coordinateur:**
```python
Toutes les secondes:
1. current_time = time.time()
2. Pour chaque processus dans active_processes:
   last_seen = last_heartbeat[process_id]
   if current_time - last_seen > 5.0:  # Timeout 5 secondes
       Marquer comme défaillant
       Déclencher la réorganisation
```

**Seuils configurables:**
- Intervalle d'envoi: 2 secondes
- Timeout de détection: 5 secondes
- Ratio de tolérance: 2.5x l'intervalle

### Réorganisation après panne

**Détection et classification:**
```python
if failed_id == 0:
    # Le coordinateur est tombé
    Déclencher nouvelle élection
else:
    # Processus ordinaire tombé
    Réorganiser les IDs
```

**Algorithme de réorganisation des IDs:**
```python
1. Retirer le processus défaillant:
   active_processes.remove(failed_id)

2. Créer un nouveau mapping:
   sorted_active = sorted(active_processes)
   old_to_new_mapping = {}
   for i, old_id in enumerate(sorted_active):
       old_to_new_mapping[old_id] = i

3. Pour chaque changement:
   if old_id != new_id:
       send IdReassignmentMessage(old_id, new_id)

4. Mettre à jour le nombre total:
   nbProcess = len(active_processes)
   broadcast ProcessCountUpdateMessage(nbProcess)
```

**Exemple de réorganisation:**
```
Avant panne: P0, P1, P2, P3, P4
P2 tombe en panne
Réorganisation:
- P0 reste P0
- P1 reste P1
- P3 devient P2
- P4 devient P3
Après: P0, P1, P2, P3
```

### Gestion de la panne du coordinateur

**Détection par les processus:**
- Absence de certains services (attribution d'ID, etc.)
- Timeout sur les requêtes au coordinateur
- Absence de heartbeat du P0

**Réélection automatique:**
```python
1. Détection collective de la panne
2. Chaque processus:
   election_in_progress = False
   received_election_numbers.clear()
   Lance request_dynamic_id()
3. Nouvelle élection complète
4. Nouveau coordinateur élu
5. Réattribution complète des IDs
```

### Adaptation du Token Ring

**Mise à jour automatique:**
```python
next_node():
1. Récupère active_processes du HeartbeatManager
2. Trie la liste
3. Trouve ma position
4. next = (position + 1) % len(active_processes)
5. Retourne active_processes[next]
```

**Propriétés:**
- Le token saute automatiquement les processus défaillants
- Pas d'intervention manuelle nécessaire
- Préserve la circulation du token

---

## Gestionnaires d'Événements

### Architecture événementielle

Le système utilise PyEventBus3 avec le mode PARALLEL pour le traitement concurrent des événements. Chaque handler est décoré avec @subscribe.

### on_broadcast(self, event: BroadcastMessage)

**Filtrage et traitement:**
```python
1. Filtrage des messages propres:
   if id_assigned and source == myId:
       return
   if not id_assigned and source == temp_id:
       return

2. Extraction du payload:
   payload = event.get_payload()

3. Traitement des messages système:
   if payload.startswith("ELECTION_NUMBER:"):
       Transmet à ElectionManager
       return
   if payload == "PING_COORDINATOR":
       if is_coordinator:
           Envoie PONG_COORDINATOR
       return
   if payload.startswith("PONG_COORDINATOR:"):
       Transmet à ElectionManager
       return

4. Messages utilisateur:
   put_message(event)  # Va dans la mailbox
```

**Messages système traités:**
- ELECTION_NUMBER: Nombres pour l'élection
- PING_COORDINATOR: Recherche de coordinateur
- PONG_COORDINATOR: Réponse du coordinateur
- PROCESS_COUNT_UPDATE: Mise à jour du nombre de processus

### on_dedicated(self, event: DedicatedMessage)

**Traitement point à point:**
```python
1. Vérification du destinataire:
   if event.dest != myId:
       return

2. Filtrage des messages propres:
   if event.source == myId:
       return

3. Insertion dans la mailbox:
   put_message(event)
```

### on_token(self, event: TokenMessage)

**Gestion complète du token:**
```python
1. Délai de visualisation:
   sleep(1.0)

2. Vérifications de sécurité:
   if not process.alive:
       return
   if source == myId:
       return
   if dest != myId:
       return

3. Réception du token:
   self.token = event.get_token()

4. Traitement selon l'état SC:
   if SC == 1:  # Demande en attente
       SC = 2
       log("Keeping token")

5. Attente de libération:
   while SC != 0:
       sleep(0.01)

6. Transmission au suivant:
   next = next_node()
   send_token(token)
```

### on_sync(self, event: SyncMessage)

**Traitement des messages de synchronisation:**
```python
1. Extraction du type et génération:
   msg_type, generation = parse_message(event)

2. Filtrage par génération:
   if generation < sync_generation:
       return  # Message obsolète

3. Traitement READY:
   if msg_type == "READY":
       if not sync_barrier:
           # Auto-jointure
           sync_generation = generation
           sync_barrier = True
           send_sync_message("READY")
       else:
           sync_ready_count += 1
           if sync_ready_count >= expected:
               send_sync_message("SYNC_RELEASE")

4. Traitement SYNC_RELEASE:
   if msg_type == "SYNC_RELEASE":
       sync_barrier = False
       sync_ready_count = 0
```

### on_heartbeat(self, event: HeartbeatMessage)

**Délégation simple:**
```python
if heartbeat_manager:
    heartbeat_manager.handle_heartbeat(event)
```

Le HeartbeatManager maintient:
- last_heartbeat[source] = timestamp
- active_processes = set(IDs actifs)

### on_id_assignment(self, event: IdAssignmentMessage)

**Réception d'ID:**
```python
if election_manager:
    election_manager.handle_id_assignment(event)
```

Traitement par l'ElectionManager:
- Vérifie si destiné à ce processus
- Assigne l'ID si pas déjà assigné
- Démarre le heartbeat

### on_id_reassignment(self, event: IdReassignmentMessage)

**Réassignation après panne:**
```python
1. Vérification:
   if event.old_id == myId:

2. Mise à jour:
   old_id = myId
   myId = event.new_id

3. Logging:
   log(f"Reassigned from P{old_id} to P{new_id}")
```

### on_process_count_update(self, event: ProcessCountUpdateMessage)

**Mise à jour du nombre de processus:**
```python
if event.source != myId:
    old_count = nbProcess
    nbProcess = event.new_count
    log(f"Process count updated from {old_count} to {nbProcess}")
```

---

## Cycle de Vie et États

### Cycle de vie complet d'un processus

**1. Phase d'initialisation**
```
Création du processus
├── Génération du temp_id (nombre aléatoire)
├── Création de l'instance Com
├── Enregistrement sur PyEventBus
└── Démarrage du thread
```

**2. Phase d'élection**
```
request_dynamic_id()
├── Recherche de coordinateur (PING)
├── Si pas de coordinateur:
│   ├── Tirage du nombre aléatoire
│   ├── Diffusion du nombre
│   ├── Collecte des nombres (1.5s)
│   └── Détermination du gagnant
└── Si coordinateur existe:
    └── Demande d'ID
```

**3. Phase d'attribution d'ID**
```
Réception de l'ID
├── Si gagnant: devient P0 (coordinateur)
├── Si perdant: reçoit ID > 0
├── Mise à jour du nom
├── id_assigned = True
└── Démarrage du heartbeat
```

**4. Phase opérationnelle**
```
Processus actif
├── Envoi de heartbeats (2s)
├── Participation au token ring
├── Communications asynchrones
├── Communications synchrones
└── Barrières de synchronisation
```

**5. Phase de panne (optionnelle)**
```
Simulation de panne
├── stop_heartbeat()
├── alive = False
└── Sortie du thread
```

**6. Phase de détection**
```
Détection par le coordinateur
├── Timeout de 5 secondes
├── Retrait de active_processes
├── Réorganisation des IDs
└── Mise à jour du token ring
```

### États du Token Ring

**Diagramme d'états:**
```
        ┌─────────┐
        │ SC = 0  │ ← release_SC()
        │  Libre  │
        └────┬────┘
             │ request_SC()
        ┌────▼────┐
        │ SC = 1  │
        │ Attente │
        └────┬────┘
             │ Token reçu
        ┌────▼────┐
        │ SC = 2  │
        │ Section │
        │Critique │
        └─────────┘
```

### États de synchronisation

**Machine à états:**
```
     ┌──────────┐
     │  Libre   │
     └─────┬────┘
           │ synchronize() ou READY reçu
     ┌─────▼────┐
     │ Barrière │
     │  Active  │
     └─────┬────┘
           │ SYNC_RELEASE reçu
     ┌─────▼────┐
     │ Libéré   │
     └──────────┘
```

### États d'élection

**Transitions:**
```
Non assigné → Recherche coordinateur → Élection → Attribution → Assigné
                    │                      │
                    └── Coordinateur ──────┘
                         existant
```

---

## Considérations de Performance et Thread Safety

### Thread Safety

**Mécanismes de protection:**

1. **Mailbox:** Mutex unique pour tous les accès
2. **Token:** token_mutex pour l'accès concurrent
3. **Heartbeat:** Lock interne dans HeartbeatManager
4. **Horloge:** Opérations atomiques

**Mode PARALLEL de PyEventBus:**
- Chaque handler s'exécute dans son propre thread
- Permet le traitement concurrent des messages
- Nécessite une synchronisation appropriée

### Optimisations

**1. Filtrage des messages système:**
- Les messages système ne polluent pas la mailbox utilisateur
- Traitement direct dans les handlers
- Économie de mémoire et de cycles CPU

**2. Gestion intelligente des logs:**
- Détection des messages dupliqués
- Agrégation des logs répétitifs
- Niveaux de log configurables

**3. Nettoyage mémoire:**
```python
# Générations de synchronisation
if len(ready_sent_for_generation) > 5:
    # Garde seulement les 5 dernières

# Structures d'élection
after election:
    received_election_numbers.clear()
    received_pongs.clear()
```

**4. Optimisation des boucles d'attente:**
```python
while condition:
    sleep(0.01)  # Évite la consommation CPU à 100%
```

### Complexité algorithmique

**Communications asynchrones:**
- Envoi: O(1)
- Réception: O(1)
- Récupération par source: O(n) où n = taille mailbox

**Synchronisation:**
- Barrière: O(p) où p = nombre de processus
- Messages READY: O(p²) messages échangés

**Élection:**
- Phase de collecte: O(p) messages
- Détermination du gagnant: O(p)
- Attribution des IDs: O(p)

**Token Ring:**
- Passage du token: O(1)
- Tour complet: O(p)

**Heartbeat:**
- Envoi: O(1) par processus
- Détection: O(p) pour le coordinateur
- Réorganisation: O(p)

### Gestion de la charge

**Limitation de la charge:**
- Heartbeat: 1 message/2s par processus
- Token: Vitesse contrôlée par délais
- Synchronisation: Génération évite les boucles

**Scalabilité:**
- Jusqu'à ~100 processus sans dégradation
- Au-delà: considérer des optimisations
  - Heartbeat hiérarchique
  - Token ring multiple
  - Synchronisation par groupes

---

## Annexes

### Types de messages

**Messages utilisateur:**
- BroadcastMessage: Diffusion à tous
- DedicatedMessage: Point à point

**Messages système:**
- TokenMessage: Circulation du token
- SyncMessage: Synchronisation (READY/RELEASE)
- HeartbeatMessage: Signal de vie
- IdRequestMessage: Demande d'ID
- IdAssignmentMessage: Attribution d'ID
- IdReassignmentMessage: Réassignation après panne
- ProcessCountUpdateMessage: Mise à jour du nombre
- BroadcastSyncMessage: Broadcast synchrone
- BroadcastSyncAckMessage: ACK de broadcast
- SendToSyncMessage: Message synchrone P2P
- SendToSyncAckMessage: ACK synchrone P2P

### Codes d'erreur et exceptions

**NotUserMessageException:**
- Tentative d'insertion d'un message système dans la mailbox
- Contient: process_id, timestamp, message_type

**TimeoutException:**
- Timeout sur communication synchrone
- Contient: durée, type d'opération

**ElectionException:**
- Problème pendant l'élection
- Causes: égalités multiples, corruption de données

### Configuration par défaut

**Timings:**
- Heartbeat interval: 2 secondes
- Heartbeat timeout: 5 secondes
- Élection collecte: 1.5 secondes
- Token visualisation: 1 seconde
- Token transfer delay: 0.5 secondes
- Sync wait loop: 0.01 seconde

**Limites:**
- Générations gardées: 5
- Nombre max processus: Non limité (pratique ~100)
- Taille mailbox: Non limitée

---

*Documentation technique complète - Module Com v3.0*
*Dernière mise à jour: 2024*