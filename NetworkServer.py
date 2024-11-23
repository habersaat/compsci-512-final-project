import time
import random
from variables import *
from leader import Leader
from follower import Follower
from candidate import Candidate
from message import Message

class NetworkServer:
    """
    Represents a server in the Raft protocol with integrated network simulation.
    Handles state transitions, message processing, and client commands with latency and retransmission.
    """

    def __init__(self, name, state, log, neighbors, latency_range=(0.01, 0.1), retransmission_chance=0.1):
        """
        Initializes the server instance with network simulation capabilities.

        :param name: Unique identifier for the server
        :param state: Initial state of the server (e.g., Follower, Leader)
        :param log: Log entries for the server
        :param neighbors: List of neighboring servers
        :param latency_range: Tuple (min_latency, max_latency) in seconds
        :param retransmission_chance: Probability [0, 1] of retransmitting a message
        """
        self._x = 0  # State machine value
        self._name = name
        self._state = state
        self._log = log
        self._board = []  # Message queue
        self._neighbors = neighbors
        self._total_nodes = 0  # Total number of nodes in the cluster
        self._commitIndex = -1
        self._currentTerm = 0
        self._lastApplied = 0
        self._lastLogIndex = -1
        self._lastLogTerm = None
        self._serverState = followerState
        self._state.set_server(self)  # Link the server to its state

        # Network simulation parameters
        self.latency_range = latency_range
        self.retransmission_chance = retransmission_chance

    # ----------------------- Message Board Operations -----------------------

    def post_message(self, message):
        """
        Adds a message to the server's message board and sorts it by timestamp.

        :param message: The message to add
        """
        self._board.append(message)
        self._board.sort(key=lambda msg: msg.timestamp, reverse=True)

    def get_message(self):
        """
        Retrieves the next message from the message board.

        :return: The next message or None if the board is empty
        """
        return self._board.pop() if self._board else None

    # ----------------------- Message Sending with Network Simulation -----------------------

    def send_message(self, message):
        """
        Broadcasts a message to all neighboring servers with simulated latency and retransmission.

        :param message: The message to broadcast
        """
        for neighbor in self._neighbors:
            if neighbor._serverState != deadState:
                message._receiver = neighbor._name
                self._simulate_network_conditions(neighbor, message)
                neighbor.post_message(message)

    def send_message_response(self, message):
        """
        Sends a direct response message to the intended recipient with simulated latency.

        :param message: The response message
        """
        for neighbor in self._neighbors:
            if neighbor._name == message.receiver:
                self._simulate_network_conditions(neighbor, message)
                neighbor.post_message(message)

    def _simulate_network_conditions(self, neighbor, message):
        """
        Simulates network latency and retransmissions for message delivery.

        :param neighbor: The receiving server
        :param message: The message to deliver
        """
        # Simulate latency
        latency = random.uniform(*self.latency_range)
        time.sleep(latency)

        # Simulate retransmission
        if random.random() < self.retransmission_chance:
            latency = random.uniform(*self.latency_range)
            time.sleep(latency)

        # Deliver the message
        neighbor.post_message(message)

    # ----------------------- Message Handling -----------------------

    def on_message(self, message):
        """
        Processes an incoming message and transitions state if necessary.

        :param message: The incoming message
        """
        # Ignore vote messages if the server is a leader
        if (message._type in {Message.RequestVoteResponse, Message.RequestVote} and
                isinstance(self._state, Leader)):
            return

        # Ignore RequestVoteResponse if the server is a follower
        if message._type == Message.RequestVoteResponse and isinstance(self._state, Follower):
            return

        # Delegate message handling to the current state
        state, response = self._state.on_message(message)

        # Transition to leader state if applicable
        if isinstance(state, Leader) and not isinstance(self._state, Leader):
            self._state = state
            self._state._send_heart_beat()

        # Update the server's state
        self._state = state

    # ----------------------- Client Command Handling -----------------------

    def on_client_command(self, message_data):
        """
        Handles a client command by forwarding it to the leader or processing it locally.

        :param message_data: The command data from the client
        """
        leader, leader_term = self._find_leader()

        # Create a client command message
        message = Message(
            sender=self._name,
            receiver=leader,
            term=leader_term,
            data={"command": message_data},
            message_type=Message.ClientCommand
        )

        # Forward the command to the leader or handle it locally
        if leader is not None:
            self.send_message_response(message)
        else:
            self._state.run_client_command(message)

    # ----------------------- Helper Methods -----------------------

    def _find_leader(self):
        """
        Identifies the current leader among neighboring servers.

        :return: Tuple (leader_name, leader_term) or (None, None) if no leader exists
        """
        for neighbor in self._neighbors:
            if isinstance(neighbor._state, Leader):
                return neighbor._name, neighbor._currentTerm
        return None, None
