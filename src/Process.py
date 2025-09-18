from re import S
from threading import Lock, Thread
from time import sleep, time
import time as time_module
from Message import BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage, RegistrationMessage, RequestMaxIdMessage, ResponseMaxIdMessage, IdRequestMessage, IdAssignmentMessage, HeartbeatMessage
from pyeventbus3.pyeventbus3 import *
from middleware.Com import Com


class Process(Thread):

    def __init__(self, name, npProcess):
        Thread.__init__(self)

        self.npProcess = npProcess
        self.myProcessName = name
        self.name = "MainThread-" + name
        self.clock = 0
        self.token = None
        self.SC = 0
        self.sync_ready_count = 0
        self.sync_barrier = False

        # Système de numérotation automatique sans variables de classe
        self.temp_id = int(time_module.time() * 1000000) % 1000000  # ID temporaire unique basé sur timestamp
        self.myId = None  # ID final assigné par le coordinateur
        self.is_coordinator = False
        self.next_id_to_assign = 0
        self.id_assigned = False
        self.pending_id_requests = []  # Liste des processus demandant un ID

        # Variables pour heartbeat et détection de pannes
        self.active_processes = set()  # Processus vivants connus
        self.last_heartbeat = {}  # Dernier heartbeat reçu de chaque processus
        self.heartbeat_interval = 2.0  # Intervalle d'envoi de heartbeat
        self.failure_timeout = 5.0  # Délai pour considérer un processus mort

        self.com = Com(self)
        self.com.nbProcess = self.npProcess

        self.alive = True
        self.start()

    # @subscribe(threadMode = Mode.PARALLEL, onEvent=BroadcastMessage)
    # def on_broadcast(self, event):
    #     if(event.source != self.name):      
    #         print(self.name + " received: " + str(event.get_payload()) + " from broadcast")
    #         self.clock = max(self.clock, event.get_stamp()) + 1

    # @subscribe(threadMode = Mode.PARALLEL, onEvent=DedicatedMessage)
    # def on_receive(self, event):
    #     if(event.get_dest() == self.myProcessName):
    #         print(self.name + " received: " + str(event.get_payload()) + " from " + str(event.source))
    #         self.clock = max(self.clock, event.get_stamp()) + 1

    # @subscribe(threadMode = Mode.PARALLEL, onEvent=TokenMessage)
    # def on_token(self, event):
    #     if(event.get_dest() == self.myId):
    #         print(self.name + " received: " + str(event.get_payload()) + " from " + str(event.source))
    #         sleep(0.1) #terminal overflow
    #         self.token = event.get_payload()
    #         if not self.SC:
    #             self.release()

    # @subscribe(threadMode = Mode.PARALLEL, onEvent=SyncMessage)
    # def on_sync(self, event):
    #     msg_type = event.get_type()

    #     if msg_type == "READY" and self.myId == 0:
    #         self.sync_ready_count += 1
    #         print(f"{self.name} received READY from process {event.source} ({self.sync_ready_count}/{self.npProcess})")

    #         if self.sync_ready_count == self.npProcess:
    #             print(f"{self.name} all processes ready, releasing synchronization barrier")
    #             self.clock = max(self.clock, event.get_stamp()) + 1
    #             SyncMessage.new_sync_message("SYNC_RELEASE", self).send(PyBus.Instance(), self)
    #             self.sync_ready_count = 0

    #     elif msg_type == "SYNC_RELEASE":
    #         print(f"{self.name} received SYNC_RELEASE, proceeding")
    #         self.clock = max(self.clock, event.get_stamp()) + 1
    #         self.sync_barrier = False


   

    # def broadcast(self, msg):
    #     if self.alive:
    #         BroadcastMessage.new_broadcast_message(msg, self).broadcast(PyBus.Instance(), self)

    # def send_to(self, msg, dest):
    #     if self.alive:
    #         DedicatedMessage.new_dedicated_message(msg, self, dest).send(PyBus.Instance(), self)

    # def send_token(self):
    #     if self.alive:
    #         TokenMessage.new_token_message(self, self.next_node()).send(PyBus.Instance(), self)
    #         self.token = None

    # def request(self):
    #     self.SC = 1
    #     while self.token == None and self.SC == 1 :
    #         pass
        
    #     print(self.name + " received token")
    #     sleep(0.5)
    #     self.release()

    # def release(self):
    #     print(self.name + " released token")
    #     self.SC = 0
    #     self.send_token()

    # def synchronize(self):
    #     print(f"{self.name} entering synchronization barrier")
    #     self.sync_barrier = True

    #     # Tous les processus envoient READY, y compris le coordinateur
    #     SyncMessage.new_sync_message("READY", self).send(PyBus.Instance(), self)

    #     if self.myId == 0:
    #         print(f"{self.name} (coordinator) waiting for other processes")

    #     while self.sync_barrier:
    #         sleep(0.01)

    #     print(f"{self.name} passed synchronization barrier, clock: {self.clock}")

    def run(self):
        # Démarrer l'attribution d'ID automatique
        if not self.id_assigned:
            self.com.initiate_id_assignment()

        # Attendre que l'ID soit assigné
        while not self.id_assigned:
            sleep(0.1)

        # Démarrer le système de heartbeat
        self.com.start_heartbeat()

        loop = 0
        while self.alive:
            print(self.name + " Loop: " + str(loop) + " clock: " + str(self.com.get_clock()))
            sleep(1)

            # Test des communications synchrones
            if loop == 3 and self.myId == 0:
                print(f"\n{self.name} === Testing broadcastSync ===")
                self.com.broadcastSync("Hello from coordinator!", 0)

            if loop == 5 and self.myId == 1:
                print(f"\n{self.name} === Testing sendToSync ===")
                self.com.sendToSync("Direct message", 2)

            if loop == 5 and self.myId == 2:
                print(f"\n{self.name} === Testing recevFromSync ===")
                received = self.com.recevFromSync(1)
                print(f"{self.name} received: {received}")

            # Test de synchronisation
            if loop == 8 and self.myId == 0:
                self.com.synchronize()

            if loop == 10 and self.myId == 1:
                self.com.synchronize()

            if loop == 7 and self.myId == 2:
                self.com.synchronize()

            loop+=1
        print(self.name + " stopped")

    def next_node(self):
        return (self.myId + 1) % self.npProcess

    def stop(self):
        print(self.name + " stopped")
        self.alive = False

    def waitStopped(self):
        self.join()
