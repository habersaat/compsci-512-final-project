import time
import random
from threading import Lock, Thread

class Network:
    def __init__(self, lag, retransmission_chance):
        """
        Initializes the network layer.

        :param lag: Maximum network latency in seconds.
        :param retransmission_chance: Probability of a message being retransmitted.
        """
        self.lag = lag
        self.retransmission_chance = retransmission_chance
        self.lock = Lock()  # For thread-safe message delivery simulation

    def send_message(self, sender, receiver, message, on_receive_callback):
        """
        Simulates sending a message with network lag and retransmission.

        :param sender: The sender server ID.
        :param receiver: The receiver server object.
        :param message: The message to send.
        :param on_receive_callback: Function to invoke when the message is received.
        """
        def delayed_send():
            # Simulate network latency
            latency = random.uniform(0, self.lag)
            time.sleep(latency)

            # Simulate retransmission
            if random.random() < self.retransmission_chance:
                retransmission_latency = random.uniform(0, self.lag)
                print(f"[Retransmitting] Message from {sender} to {receiver.server_id} after {retransmission_latency:.3f} seconds delay")
                time.sleep(retransmission_latency)

            # Deliver the message to the receiver
            with self.lock:
                on_receive_callback(receiver, message)

        # Run the delayed send in a separate thread
        Thread(target=delayed_send, daemon=True).start()

    def broadcast_message(self, sender, neighbors, message, on_receive_callback):
        """
        Broadcasts a message to all neighbors with network latency and retransmission.

        :param sender: The sender server ID.
        :param neighbors: List of neighboring server objects.
        :param message: The message to broadcast.
        :param on_receive_callback: Function to invoke when the message is received.
        """
        for neighbor in neighbors:
            self.send_message(sender, neighbor, message, on_receive_callback)
