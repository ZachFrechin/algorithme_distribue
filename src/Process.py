from re import S
from threading import Lock, Thread
from time import sleep, time
from Message import BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage, RegistrationMessage, RequestMaxIdMessage, ResponseMaxIdMessage
from pyeventbus3.pyeventbus3 import *
from middleware.Com import Com


class Process(Thread):
        
    nbProcessCreated = 0
    
    def __init__(self, name, npProcess):
        Thread.__init__(self)

        self.npProcess = npProcess
        self.myId = Process.nbProcessCreated
        self.myProcessName = name
        self.name = "MainThread-" + name
        self.clock = 0
        self.token = None
        self.SC = 0
        self.sync_ready_count = 0
        self.sync_barrier = False
        
        self.magic_number = 99999999999999
        self.random_number = None

        # État pour le nouveau système d'attribution d'ID
        self.is_max_id = False  # Suis-je le détenteur de l'ID max ?
        self.id_assignment_mutex = Lock()  # Mutex pour attribution ID

        self.com = Com(self)

        Process.nbProcessCreated += 1

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
        loop = 0
        while self.alive:
            print(self.name + " Loop: " + str(loop) + " clock: " + str(self.com.get_clock()))
            sleep(1)

            
                



            loop+=1
        print(self.name + " stopped")

    def next_node(self):
        return (self.myId + 1) % self.npProcess

    def stop(self):
        print(self.name + " stopped")
        self.alive = False

    def waitStopped(self):
        self.join()
