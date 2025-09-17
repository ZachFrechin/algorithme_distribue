from threading import Lock, Thread
from time import sleep
from Message import BroadcastMessage, DedicatedMessage, TokenMessage, SyncMessage
from pyeventbus3.pyeventbus3 import *
from middleware.Com_AsyncSafe import Com


class Process(Thread):
    """
    Async-safe process implementation for distributed systems.

    Key improvements:
    1. Thread-safe mailbox integration
    2. Proper logical clock synchronization
    3. Deadlock-free synchronization barriers
    4. Atomic critical section management
    """

    nbProcessCreated = 0

    def __init__(self, name, npProcess):
        Thread.__init__(self)

        self.npProcess = npProcess
        self.myId = Process.nbProcessCreated
        self.myProcessName = name
        self.name = "MainThread-" + name

        # Thread-safe communication layer
        self.com = Com(process=self)

        # Synchronization state with proper locking
        self._state_lock = Lock()
        self.token = None
        self.SC = 0
        self.sync_ready_count = 0
        self.sync_barrier = False

        Process.nbProcessCreated += 1
        PyBus.Instance().register(self, self)

        self.alive = True
        self.start()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastMessage)
    def on_broadcast(self, event):
        """
        Thread-safe broadcast message handler.
        Integrates with async-safe mailbox and logical clock.
        """
        if event.source != self.name:
            print(f"{self.name} received broadcast: {event.get_payload()} from {event.source}")

            # Update logical clock atomically
            self.com._update_clock_on_receive(event.get_stamp())

            # Store message in thread-safe mailbox
            try:
                self.com.put_message(event)
            except TypeError as e:
                print(f"Error storing broadcast message: {e}")

    @subscribe(threadMode=Mode.PARALLEL, onEvent=DedicatedMessage)
    def on_receive(self, event):
        """
        Thread-safe dedicated message handler.
        Only processes messages destined for this process.
        """
        if event.get_dest() == self.myProcessName:
            print(f"{self.name} received: {event.get_payload()} from {event.source}")

            # Update logical clock atomically
            self.com._update_clock_on_receive(event.get_stamp())

            # Store message in thread-safe mailbox
            try:
                self.com.put_message(event)
            except TypeError as e:
                print(f"Error storing dedicated message: {e}")

    @subscribe(threadMode=Mode.PARALLEL, onEvent=TokenMessage)
    def on_token(self, event):
        """
        Thread-safe token handler for mutual exclusion.
        Atomic token assignment prevents race conditions.
        """
        if event.get_dest() == self.myId:
            print(f"{self.name} received token from {event.source}")
            sleep(0.1)  # Terminal overflow prevention

            # Update logical clock
            self.com._update_clock_on_receive(event.get_stamp())

            # Atomic token assignment and critical section check
            with self._state_lock:
                self.token = event.get_payload()
                should_release = not self.SC

            # Release token immediately if not in critical section
            if should_release:
                self.release()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=SyncMessage)
    def on_sync(self, event):
        """
        Thread-safe synchronization barrier handler.
        Prevents race conditions in barrier coordination.
        """
        msg_type = event.get_type()

        # Update logical clock
        self.com._update_clock_on_receive(event.get_stamp())

        if msg_type == "READY" and self.myId == 0:
            # Coordinator: atomically count ready messages
            with self._state_lock:
                self.sync_ready_count += 1
                print(f"{self.name} received READY from {event.source} "
                      f"({self.sync_ready_count}/{self.npProcess})")

                # Check if all processes are ready
                if self.sync_ready_count == self.npProcess:
                    print(f"{self.name} all processes ready, releasing barrier")

                    # Reset count immediately to prevent double-release
                    self.sync_ready_count = 0

                    # Send release outside the lock to avoid deadlock
                    should_send_release = True
                else:
                    should_send_release = False

            # Send release message outside the lock
            if should_send_release:
                self.com.inc_clock()
                SyncMessage.new_sync_message("SYNC_RELEASE", self.com.get_clock(), self).send(PyBus.Instance())

        elif msg_type == "SYNC_RELEASE":
            print(f"{self.name} received SYNC_RELEASE, proceeding")

            # Atomic barrier release
            with self._state_lock:
                self.sync_barrier = False

    def broadcast(self, msg):
        """Send broadcast message with proper clock update."""
        if self.alive:
            self.com.inc_clock()
            BroadcastMessage.new_broadcast_message(
                msg, self.com.get_clock(), self.name
            ).broadcast(PyBus.Instance())

    def send_to(self, msg, dest):
        """Send dedicated message with proper clock update."""
        if self.alive:
            self.com.inc_clock()
            DedicatedMessage.new_dedicated_message(
                msg, self.com.get_clock(), dest, self.name
            ).send(PyBus.Instance())

    def send_token(self):
        """Send token to next process in ring."""
        if self.alive:
            self.com.inc_clock()
            TokenMessage.new_token_message(
                self.token, self.com.get_clock(), self.next_node(), self.myId
            ).send(PyBus.Instance())

            # Clear token after sending
            with self._state_lock:
                self.token = None

    def request(self):
        """
        Request critical section access.
        Uses busy-waiting but with proper atomic checks.
        """
        with self._state_lock:
            self.SC = 1

        # Busy wait for token with atomic check
        while True:
            with self._state_lock:
                if self.token is not None or self.SC == 0:
                    break
            sleep(0.001)  # Small sleep to reduce CPU usage

        print(f"{self.name} entered critical section")
        sleep(0.5)  # Simulate critical section work
        self.release()

    def release(self):
        """
        Release critical section and send token to next process.
        Atomic state update prevents races.
        """
        print(f"{self.name} released critical section")

        with self._state_lock:
            self.SC = 0

        self.send_token()

    def synchronize(self):
        """
        Distributed synchronization barrier.
        All processes wait until coordinator releases them.
        """
        print(f"{self.name} entering synchronization barrier")

        # Set barrier flag atomically
        with self._state_lock:
            self.sync_barrier = True

        # Send READY message to coordinator
        self.com.inc_clock()
        SyncMessage.new_sync_message("READY", self.com.get_clock(), self.name).send(PyBus.Instance())

        if self.myId == 0:
            print(f"{self.name} (coordinator) waiting for other processes")

        # Wait for barrier release with atomic check
        while True:
            with self._state_lock:
                if not self.sync_barrier:
                    break
            sleep(0.01)

        print(f"{self.name} passed synchronization barrier, clock: {self.com.get_clock()}")

    def run(self):
        """Main process loop with async-safe operations."""
        loop = 0
        while self.alive:
            print(f"{self.name} Loop: {loop} clock: {self.com.get_clock()}")
            sleep(1)

            if loop == 1:
                # Uncomment to test synchronization
                # print(f"{self.name} calling synchronize() at loop {loop}")
                # self.synchronize()
                pass

            if loop >= 3:
                break

            if self.myProcessName == "P0" and loop == 0:
                # Example operations
                # self.broadcast("hello")
                # self.send_to("hello", "P2")
                # self.send_token()
                pass

            if self.myProcessName == "P2" and loop == 2:
                # self.request()
                pass

            # Process any pending messages from mailbox
            self._process_pending_messages()

            loop += 1
        print(f"{self.name} stopped")

    def _process_pending_messages(self):
        """
        Process messages from mailbox (example usage).
        This demonstrates how to consume messages safely.
        """
        messages = self.com.get_messages()
        for message in messages:
            # Process each message based on type
            if isinstance(message, BroadcastMessage):
                print(f"{self.name} processing broadcast: {message.get_payload()}")
            elif isinstance(message, DedicatedMessage):
                print(f"{self.name} processing dedicated: {message.get_payload()}")

    def next_node(self):
        """Calculate next node in token ring."""
        return (self.myId + 1) % self.npProcess

    def stop(self):
        """Stop the process gracefully."""
        print(f"{self.name} stopping")
        self.alive = False

    def waitStopped(self):
        """Wait for process thread to complete."""
        self.join()

    def get_mailbox_stats(self):
        """Get mailbox statistics for monitoring."""
        return self.com.get_statistics()