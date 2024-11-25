from server_roles import Leader, Follower, ServerState
from message import Message, MessageType
from random import randint
import random
import time
from queue import PriorityQueue
from threading import Lock

class Server:
    def __init__(self, node_id, role, neighbors=set(), log=[], commit_index=-1, latency_range=(0.0, 0.0), retransmission_chance=0.0):
        self.id = node_id                               # Unique identifier for the server
        self.role = role                                # Role of the server (Leader, Follower, Candidate)
        self.server_state = ServerState.FOLLOWER        # Default server state is Follower
        self.neighbors = neighbors                      # List of peer servers
        self.log = log                                  # Log entries for Raft
        self.message_queue = PriorityQueue()            # Priority Queue for incoming messages (sorted by unpack_time)
        self.message_queue_lock = Lock()                # Lock for the message queue
        self.total_nodes = 0                            # Total number of nodes in the cluster
        self.active_nodes = 0                           # Number of active nodes in the cluster

        # Indexes for Raft Algorithm
        self.commit_index = commit_index                # Index of the last committed log entry
        self.current_term = 0                           # Current Raft term
        self.last_log_index = -1                        # Index of the last log entry
        self.last_log_term = None                       # Term of the last log entry

        # Network simulation parameters
        self.latency_range = latency_range                  # Range of network latency for messages
        self.retransmission_chance = retransmission_chance  # Chance of retransmitting a message

        # Set the server for the role
        self.role.assign_to_server(self)


    def simulate_network_conditions(self, neighbor, message):
        """
        Simulates network latency and retransmissions with exponentially increasing backoff (like TCP).

        :param neighbor: The receiving server
        :param message: The message to deliver
        """
        base_latency = random.uniform(*self.latency_range)
        unpack_time = time.time() + base_latency

        # Simulate retransmissions with exponential backoff
        retransmission_count = 0
        while random.random() < self.retransmission_chance:
            retransmission_delay = base_latency * (2 ** retransmission_count)
            unpack_time += retransmission_delay
            retransmission_count += 1

        # Update the message's unpack_time
        message.set_unpack_time(unpack_time)

        # Post the message to the neighbor's queue
        neighbor.post_message(message)

    def get_leader(self):
        """
        Returns the leader node if available, else None.
        """
        return next((n for n in self.neighbors if isinstance(n.role, Leader)), None)
    
    def transition_to_leader(self, new_state):
        """
        Transitions to the Leader state if the new state is a Leader.
        """
        self.role = new_state
        self.role.send_heartbeat()

    def post_message(self, message):
        """
        Adds a message to the queue and sorts by unpack_time (latest first).
        """
        self.message_queue.put((message.unpack_time, message))  # Insert with priority

    def process_message(self):
        """
        Returns the next ready message if its unpack_time has passed.
        """
        if not self.message_queue.empty():
            unpack_time, message = self.message_queue.queue[0]  # Peek at the first item
            message_timestamp = message.timestamp
            if time.time() > unpack_time:
                # print(f"Server {self.id} received message with latency {unpack_time - message_timestamp:.3f}s")
                return self.message_queue.get()[1]  # Return the message
        return None
    
    def send_message(self, message, target_id=None):
        """
        Sends a message to a specific neighbor or broadcasts to all neighbors.
        """

        if target_id:
            # Send to a specific neighbor
            for neighbor in self.neighbors:
                if neighbor.id == message.dst:
                    self.simulate_network_conditions(neighbor, message)
                    break
        else:
            # Broadcast to all neighbors
            for neighbor in self.neighbors:
                if neighbor.server_state != ServerState.DEAD:
                    message.dst = neighbor.id
                    self.simulate_network_conditions(neighbor, message)

    def handle_message(self, message):
        """
        Processes an incoming message based on the server's current role.
        """
        # Ignore RequestVote-related messages if the server is a Leader or Follower
        if message.type in {MessageType.RequestVoteResponse, MessageType.RequestVote} and isinstance(self.role, Leader):
            return
        if message.type == MessageType.RequestVoteResponse and isinstance(self.role, Follower):
            return

        # Delegate message handling to the current role
        role_response = self.role.handle_message(message)
        new_state = role_response[0] if role_response else self.role

        # Transition to Leader if the state changes
        if isinstance(new_state, Leader) and not isinstance(self.role, Leader):
            self.transition_to_leader(new_state)

    def handle_client_command(self, command):
        """
        Forwards a client command to the current leader if available.
        Drops the command if no leader is found.
        """
        # Locate the current leader among neighbors
        leader = self.get_leader()

        if leader:
            # Forward the command to the leader
            message = Message(
                source=self.id,
                destination=leader.id,
                term=leader.current_term,
                payload={"command": command},
                message_type=MessageType.ClientCommand,
            )
            self.send_message(message, target_id=leader.id)
        else:
            # No leader found, command cannot be processed
            # print(f"Command dropped: {command}. No leader available.")
            pass