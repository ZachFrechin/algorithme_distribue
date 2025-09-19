# 📚 Documentation Complète du Module Com - API Détaillée

## 🎯 Vue d'ensemble

Le module `Com` est la classe principale du middleware de communication distribué. Cette documentation détaille **chaque méthode, chaque attribut et leur fonctionnement exact**.

---

## 🏗️ Classe Com - Constructeur et Attributs

### `__init__(self, process=None)`

**Description**: Initialise une nouvelle instance de Com avec tous ses composants.

**Paramètres**:
- `process` (Process|None): Instance du processus parent

**Attributs créés**:

#### 🕐 Gestion du temps et logs
- `logical_clock` (LogicalClock): Instance de l'horloge logique de Lamport
- `logger` (ProcessLogger): Gestionnaire de logs avec formatage et couleurs

#### 🗳️ Gestion des élections et IDs
- `election_manager` (ElectionManager): Gestionnaire des élections de leader et attribution d'IDs
- `process` (Process): Référence vers le processus parent

#### 📬 Communication asynchrone
- `mailbox_manager` (MailBoxManager): Gestionnaire de la boîte aux lettres pour messages utilisateur

#### ⏱️ Communication synchrone
- `sync_comm` (SyncCommunication): Gestionnaire des communications synchrones bloquantes

#### 💓 Détection de pannes
- `heartbeat_manager` (HeartbeatManager): Gestionnaire de heartbeat et détection de pannes

#### 🎫 Token Ring et Section Critique
- `token` (Token|None): Token actuellement détenu (None si pas de token)
- `token_mutex` (Lock): Mutex pour accès thread-safe au token
- `SC` (int): État de section critique (0=libre, 1=demandé, 2=possédé)

#### 🔢 Compteurs et état
- `nbProcess` (int): Nombre total de processus actifs
- `sync_ready_count` (int): Compteur de processus prêts pour synchronisation
- `sync_barrier` (bool): True si en cours de synchronisation
- `sync_generation` (int): Numéro de génération de la barrière de synchronisation
- `ready_sent_for_generation` (set): Générations pour lesquelles READY a été envoyé

---

## ⏰ Méthodes d'Horloge Logique

### `inc_clock(self) -> int`
**Description**: Incrémente l'horloge logique et retourne la nouvelle valeur.
**Retour**: Nouvelle valeur de l'horloge
**Usage**: Automatique lors des envois, ou manuel pour événements locaux

### `get_clock(self) -> int`
**Description**: Retourne la valeur actuelle de l'horloge logique.
**Retour**: Valeur actuelle de l'horloge

### `_update_clock_on_receive(self, clock) -> int`
**Description**: Met à jour l'horloge selon l'algorithme de Lamport lors de réception.
**Paramètres**: `clock` (int) - Timestamp du message reçu
**Retour**: Nouvelle valeur de l'horloge
**Algorithme**: `new_clock = max(local_clock, received_clock) + 1`

---

## 📝 Méthodes de Logging

### `log(self, message, coordinator_only=False)`
**Description**: Enregistre un message avec format `[ID - NAME - CLOCK] : MESSAGE`
**Paramètres**:
- `message` (str): Message à logger
- `coordinator_only` (bool): Si True, affiche seulement si process est coordinateur

---

## 📬 API MailBox (Messages Asynchrones)

### Insertion de messages

#### `put_message(self, message: Message)`
**Description**: Ajoute un message dans la boîte aux lettres
**Paramètres**: `message` - Message à ajouter (doit être USER_MESSAGE)
**Exceptions**: `NotUserMessageException` si message système

### Récupération de messages

#### `get_message(self) -> Message|None`
**Description**: Récupère et retire le premier message (FIFO)
**Retour**: Premier message ou None si vide

#### `get_messages(self) -> List[Message]`
**Description**: Récupère et retire tous les messages
**Retour**: Liste de tous les messages (vide la mailbox)

#### `get_message_from(self, source) -> Message|None`
**Description**: Récupère le premier message d'une source spécifique
**Paramètres**: `source` (int) - ID du processus émetteur
**Retour**: Premier message de cette source ou None

#### `get_messages_from(self, source) -> List[Message]`
**Description**: Récupère tous les messages d'une source spécifique
**Paramètres**: `source` (int) - ID du processus émetteur
**Retour**: Liste des messages de cette source

#### `get_message_type(self, message_type) -> Message|None`
**Description**: Récupère le premier message d'un type donné
**Paramètres**: `message_type` (str) - "broadcast" ou "dedicated"
**Retour**: Premier message du type ou None

#### `get_messages_type(self, message_type) -> List[Message]`
**Description**: Récupère tous les messages d'un type donné
**Paramètres**: `message_type` (str) - "broadcast" ou "dedicated"
**Retour**: Liste des messages du type

### Inspection de la mailbox

#### `has_messages(self) -> bool`
**Description**: Vérifie s'il y a des messages en attente
**Retour**: True si mailbox non vide

---

## 📡 Communications Asynchrones - Envoi

### `broadcast(self, payload)`
**Description**: Diffuse un message à tous les processus de manière asynchrone
**Paramètres**: `payload` (str) - Contenu du message
**Comportement**:
1. Incrémente l'horloge logique
2. Crée un BroadcastMessage
3. Diffuse via PyEventBus
4. N'attend pas de confirmation

### `send_to(self, payload, dest)`
**Description**: Envoie un message à un processus spécifique de manière asynchrone
**Paramètres**:
- `payload` (str) - Contenu du message
- `dest` (int) - ID du processus destinataire
**Comportement**:
1. Incrémente l'horloge logique
2. Crée un DedicatedMessage
3. Envoie via PyEventBus
4. N'attend pas de confirmation

---

## 📡 Communications Synchrones (Bloquantes)

### `broadcastSync(self, payload, sender_id)`
**Description**: Diffuse un message de manière synchrone - bloque jusqu'aux ACKs
**Paramètres**:
- `payload` (str) - Contenu du message
- `sender_id` (int) - ID de l'émetteur
**Comportement**:
1. Envoie le message broadcast
2. Attend les accusés de réception de tous les processus actifs
3. Retourne uniquement quand tous ont accusé réception

### `sendToSync(self, payload, dest)`
**Description**: Envoie un message synchrone point-à-point
**Paramètres**:
- `payload` (str) - Contenu du message
- `dest` (int) - ID du destinataire
**Comportement**: Bloque jusqu'à réception de l'ACK du destinataire

### `recevFromSync(self, source) -> str`
**Description**: Réception synchrone d'un message d'une source spécifique
**Paramètres**: `source` (int) - ID de l'émetteur attendu
**Retour**: Contenu du message reçu
**Comportement**: Bloque jusqu'à réception d'un message de cette source

---

## 🎫 Token Ring et Section Critique

### États de Section Critique (attribut SC)
- `0`: Libre (pas de demande)
- `1`: Demande en cours (attend le token)
- `2`: Possède le token (en section critique)

### `request_SC(self)`
**Description**: Demande l'accès à la section critique - BLOQUE jusqu'à obtention
**Comportement**:
1. Met SC = 1 (demande)
2. Boucle d'attente jusqu'à SC = 2 (obtention du token)
3. Retourne quand l'accès est accordé

### `release_SC(self)`
**Description**: Libère la section critique et passe le token au suivant
**Comportement**:
1. Met SC = 0 (libération)
2. Le token sera automatiquement transféré au processus suivant

### `send_token(self, token=None)`
**Description**: Envoie le token au processus suivant dans l'anneau
**Paramètres**: `token` (Token|None) - Token à transférer (None = mon token)
**Comportement**:
1. Détermine le prochain processus avec `next_node()`
2. Crée un TokenMessage
3. Envoie via PyEventBus
4. Ajoute un délai de 0.5s pour visualisation

---

## 🚦 Barrières de Synchronisation

### `synchronize(self)`
**Description**: Point de synchronisation - tous les processus doivent y arriver
**Algorithme**:
1. Incrémente `sync_generation` (nouvelle barrière)
2. Met `sync_barrier = True`
3. Envoie message "READY"
4. Attend jusqu'à ce que tous les processus soient prêts
5. Retourne quand "SYNC_RELEASE" est reçu

**Mécanisme**:
- Chaque processus envoie "READY"
- Le coordinateur compte les "READY"
- Quand tous sont prêts, diffuse "SYNC_RELEASE"
- Tous les processus sortent de l'attente

### Attributs de synchronisation

#### `sync_generation` (int)
**Description**: Numéro de génération de la barrière actuelle
**Usage**: Évite les confusions entre différentes barrières simultanées

#### `sync_ready_count` (int)
**Description**: Nombre de processus qui ont envoyé "READY" pour la barrière courante

#### `sync_barrier` (bool)
**Description**: True si le processus est en attente de synchronisation

#### `ready_sent_for_generation` (set)
**Description**: Ensemble des générations pour lesquelles ce processus a envoyé "READY"
**Usage**: Évite l'envoi multiple de "READY" pour la même barrière

---

## 🗳️ Système d'Élection et Attribution d'IDs

### `request_dynamic_id(self)`
**Description**: Demande l'attribution d'un ID unique via le système d'élection
**Algorithme**:
1. Vérifie s'il existe déjà un coordinateur (PING_COORDINATOR)
2. Si oui, demande un ID au coordinateur existant
3. Si non, lance une élection (nombres aléatoires)
4. Le plus grand nombre devient coordinateur (ID=0)
5. Le coordinateur assigne les IDs aux autres participants

**Élection par nombres aléatoires**:
- Chaque processus tire un nombre entre 1 et 1,000,000
- Diffuse son nombre via "ELECTION_NUMBER:temp_id:number"
- Attend 1.5 secondes pour collecter tous les nombres
- Détermine le gagnant (plus grand nombre)
- En cas d'égalité: plus petit temp_id gagne

---

## 💓 Système de Heartbeat et Détection de Pannes

### `start_heartbeat(self)`
**Description**: Démarre l'envoi périodique de heartbeats
**Comportement**:
- Envoi automatique toutes les 2 secondes
- Appelé automatiquement après attribution d'ID

### `stop_heartbeat(self)`
**Description**: Arrête l'envoi de heartbeats (simule une panne)
**Usage**: Pour simuler une panne de processus

### Détection de pannes
**Mécanisme**:
- Chaque processus envoie un heartbeat toutes les 2 secondes
- Le coordinateur surveille les derniers heartbeats reçus
- Si pas de heartbeat pendant 5 secondes → processus considéré comme défaillant
- Déclenchement automatique de la réorganisation des IDs

### Réorganisation après panne
**Algorithme**:
1. Détection de la panne par timeout de heartbeat
2. Suppression du processus défaillant de la liste active
3. Réorganisation des IDs pour combler les trous (0,1,2,3 → 0,1,2 si P2 tombe)
4. Envoi de messages `IdReassignmentMessage` aux processus concernés
5. Mise à jour du nombre total de processus

---

## 🔧 Gestionnaires d'Événements (Handlers)

### `@subscribe` - Réception de Messages

#### `on_broadcast(self, event: BroadcastMessage)`
**Description**: Handler pour tous les messages broadcast
**Filtrage**:
- Ignore ses propres messages
- Traite les messages système (élection, ping, etc.)
- Met les messages utilisateur dans la mailbox

#### `on_dedicated(self, event: DedicatedMessage)`
**Description**: Handler pour les messages point-à-point
**Filtrage**:
- Vérifie que `event.dest == self.process.myId`
- Ignore ses propres messages
- Met le message dans la mailbox si destiné à ce processus

#### `on_token(self, event: TokenMessage)`
**Description**: Handler pour la réception de tokens
**Algorithme**:
1. Vérifie que le token nous est destiné
2. Si demande de section critique (SC=1) → garde le token (SC=2)
3. Sinon, transfère immédiatement au suivant
4. Délai d'1 seconde pour visualisation

#### `on_sync(self, event: SyncMessage)`
**Description**: Handler pour les messages de synchronisation
**Types de messages**:
- "READY": Processus prêt pour synchronisation
- "SYNC_RELEASE": Coordinateur libère la barrière

#### `on_heartbeat(self, event: HeartbeatMessage)`
**Description**: Handler pour les messages de heartbeat
**Délègue**: Transmet à `heartbeat_manager.handle_heartbeat()`

#### `on_id_assignment(self, event: IdAssignmentMessage)`
**Description**: Handler pour l'attribution d'IDs
**Délègue**: Transmet à `election_manager.handle_id_assignment()`

#### `on_id_reassignment(self, event: IdReassignmentMessage)`
**Description**: Handler pour la réassignation d'IDs après panne
**Comportement**: Met à jour `process.myId` si le message nous concerne

#### `on_process_count_update(self, event: ProcessCountUpdateMessage)`
**Description**: Handler pour la mise à jour du nombre de processus
**Comportement**: Met à jour `self.nbProcess`

---

## 🏗️ Architecture des Composants

### LogicalClock
**Rôle**: Implémentation de l'horloge logique de Lamport
**Méthodes**:
- `increment()`: Incrémente l'horloge locale
- `get_time()`: Retourne l'heure actuelle
- `update_on_receive(timestamp)`: Mise à jour à la réception

### ProcessLogger
**Rôle**: Système de logs avec formatage et couleurs
**Format**: `[ID - NAME - CLOCK] : MESSAGE`
**Couleurs**:
- Bleu: IDs temporaires
- Rose: IDs d'autres processus
- Jaune: Messages token/section critique
- Vert: Succès
- Rouge: Erreurs/pannes

### ElectionManager
**Rôle**: Gestion des élections et attribution d'IDs
**Fonctions**:
- Détection de coordinateur existant
- Élection par nombres aléatoires
- Attribution automatique d'IDs
- Gestion des processus tardifs

### SyncCommunication
**Rôle**: Communications synchrones bloquantes
**Fonctions**:
- BroadcastSync avec attente d'ACKs
- SendToSync/ReceiveFromSync point-à-point
- Gestion des timeouts

### MailBoxManager
**Rôle**: Gestion thread-safe de la boîte aux lettres
**Fonctions**:
- Stockage FIFO des messages
- Filtrage par source et type
- Protection par mutex

### HeartbeatManager
**Rôle**: Détection de pannes et réorganisation
**Fonctions**:
- Envoi périodique de heartbeats
- Surveillance des processus actifs
- Détection de pannes par timeout
- Réorganisation automatique des IDs

---

## 🔄 Cycle de Vie et États

### États d'un processus
1. **Création**: Génère temp_id, rejoint PyEventBus
2. **Élection**: Participe à l'élection de leader
3. **Attribution ID**: Reçoit ID permanent du coordinateur
4. **Opérationnel**: Peut communiquer, heartbeat actif
5. **Panne simulée**: Arrêt heartbeat, déconnexion
6. **Détection**: Autres processus détectent la panne
7. **Réorganisation**: IDs réassignés pour combler les trous

### États de Token Ring
- **Circulation libre**: Token passe de processus en processus
- **Demande**: Processus demande section critique (SC=1)
- **Attribution**: Processus reçoit token (SC=2)
- **Section critique**: Accès exclusif aux ressources
- **Libération**: Token transféré au suivant (SC=0)

### États de Synchronisation
- **Libre**: Pas de synchronisation en cours
- **Initié**: Un processus appelle `synchronize()`
- **Propagation**: Autres processus rejoignent automatiquement
- **Attente**: Tous attendent que tous soient prêts
- **Libération**: Coordinateur envoie SYNC_RELEASE
- **Terminé**: Tous reprennent l'exécution

---

## ⚡ Considérations de Performance

### Thread Safety
- Tous les accès à la mailbox protégés par mutex
- Handlers d'événements en Mode.PARALLEL
- Token Ring thread-safe avec verrous appropriés

### Optimisations
- Messages système ne polluent pas la mailbox utilisateur
- Filtrage intelligent des doublons dans les logs
- Nettoyage périodique des anciennes générations de sync
- Heartbeats optimisés (envoi toutes les 2s, timeout à 5s)

### Gestion Mémoire
- Mailbox vidée par l'utilisateur (pas d'accumulation)
- Nettoyage automatique des structures temporaires d'élection
- Limite sur les générations de synchronisation gardées en mémoire

---

*Documentation complète du module Com - Version 3.0*
*Tous les attributs, méthodes et comportements détaillés*