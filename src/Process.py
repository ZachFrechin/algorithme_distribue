from re import S
from threading import Lock, Thread
from time import sleep, time
from Message import BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage
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
        self.com = Com(self)

        Process.nbProcessCreated += 1
        PyBus.Instance().register(self, self)

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

    def get_an_id(self, blacklist_number = None):
        import random  # Fix missing import

        # Generate random number
        rand_num = random.randint(0, self.magic_number)
        while rand_num == blacklist_number:
            rand_num = random.randint(0, self.magic_number)

        # Broadcast the random number
        self.com.broadcast(rand_num)
        sleep(2)

        messages = self.com.get_messages_type("broadcast")
        if len(messages) == 0:
            raise ValueError("No message received, no connection possible")

        # Collect all random numbers from other processes
        random_array = [message.payload for message in messages]
        random_array = sorted(random_array)

        # Check for collision
        if rand_num in random_array:
            print(f"{self.name} collision detected with {rand_num}, retrying...")
            return self.get_an_id(rand_num)  # Fix: return result of recursive call

        self.myId = rand_num
        print(f"{self.name} assigned random ID: {self.myId}")
        return True

    def get_sequential_id(self):
        """
        NEW METHOD: Distributed sequential ID assignment using leader-based consensus
        Achieves consecutive numbering starting from 0 without class variables
        """
        from time import time

        # Phase 1: Leader Election using deterministic selection
        election_payload = {
            "type": "LEADER_ELECTION",
            "candidate": self.name,
            "timestamp": time(),
            "process_count": self.npProcess
        }

        self.com.broadcast(election_payload)
        sleep(1.5)  # Allow all election messages to propagate

        # Collect all election messages
        messages = self.com.get_messages_type("broadcast")
        election_messages = []

        for msg in messages:
            if (isinstance(msg.payload, dict) and
                msg.payload.get("type") == "LEADER_ELECTION"):
                election_messages.append(msg)

        # Determine leader (process with lexicographically smallest name)
        all_candidates = [(self.name, time())]  # Include self
        for msg in election_messages:
            all_candidates.append((msg.payload["candidate"], msg.payload["timestamp"]))

        # Sort by name for deterministic leader selection
        all_candidates.sort(key=lambda x: x[0])
        leader_name = all_candidates[0][0]

        print(f"{self.name} elected leader: {leader_name}")

        # Phase 2: ID Assignment
        if self.name == leader_name:
            # Leader assigns IDs
            assigned_id = self._coordinate_sequential_assignment(all_candidates)
        else:
            # Follower waits for assignment
            assigned_id = self._wait_for_sequential_assignment(leader_name)

        self.myId = assigned_id
        print(f"{self.name} assigned sequential ID: {self.myId}")

        return assigned_id

    def _coordinate_sequential_assignment(self, all_candidates):
        """Leader coordinates sequential ID assignment"""

        # Create deterministic ordering
        process_names = [candidate[0] for candidate in all_candidates]
        process_names = sorted(set(process_names))  # Remove duplicates and sort

        # Broadcast ID assignments
        for i, process_name in enumerate(process_names):
            assignment_payload = {
                "type": "ID_ASSIGNMENT",
                "target": process_name,
                "assigned_id": i,
                "coordinator": self.name
            }
            self.com.broadcast(assignment_payload)

        # Leader gets ID 0 (first in sorted order)
        leader_id = process_names.index(self.name)
        print(f"Leader {self.name} coordinated assignments: {list(enumerate(process_names))}")

        return leader_id

    def _wait_for_sequential_assignment(self, leader_name):
        """Wait for ID assignment from leader"""

        timeout_start = time()
        timeout_duration = 5.0

        while time() - timeout_start < timeout_duration:
            sleep(0.1)

            messages = self.com.get_messages_type("broadcast")
            for msg in messages:
                if (isinstance(msg.payload, dict) and
                    msg.payload.get("type") == "ID_ASSIGNMENT" and
                    msg.payload.get("target") == self.name):

                    assigned_id = msg.payload["assigned_id"]
                    print(f"{self.name} received ID {assigned_id} from leader {leader_name}")
                    return assigned_id

        raise TimeoutError(f"{self.name} timeout waiting for ID assignment from {leader_name}")

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

            if loop == 0:
                # Use sequential ID assignment instead of random
                self.get_sequential_id()

            print(self.name + " myId: " + str(self.myId))

            loop+=1
        print(self.name + " stopped")

    def next_node(self):
        return (self.myId + 1) % self.npProcess

    def stop(self):
        print(self.name + " stopped")
        self.alive = False

    def waitStopped(self):
        self.join()
