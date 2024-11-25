import time
import random
from message import Message, MessageType
from random import randint
from collections import defaultdict
from enum import Enum, auto

class ServerState(Enum):
    DEAD = auto()
    FOLLOWER = auto()
    RESUME = auto()
    CANDIDATE = auto()
    LEADER = auto()
    JOINING = auto()

class Role:
    def assign_to_server(self, server):
        """
        Assigns the role to a server instance and initializes any necessary state
        """
        self.server = server

    def check_term_mismatch(self, message):
        """
        Handles a term mismatch between the server and an incoming message.
        """
        if message.term > self.server.current_term:
            self.server.current_term = message.term
            return True
        elif message.term < self.server.current_term:
            self.send_response(message, is_accepted=False)
            return True
        return False

    def handle_message(self, message):
        """
        Processes an incoming message based on its type.
        Updates the server's term if necessary and delegates the message to the appropriate handler.
        """
        # Check for term mismatch and handle it if necessary
        self.check_term_mismatch(message)

        # Map message types to corresponding handler methods
        message_handlers = {
            MessageType.AppendEntries: self.process_append_entries,
            MessageType.RequestVote: self.process_vote_request,
            MessageType.RequestVoteResponse: self.process_vote_received,
            MessageType.ClientResponse: self.process_response,
            MessageType.ClientCommand: self.execute_client_command,
            MessageType.RequestToJoin: self.process_join_request,
            MessageType.AddToCluster: self.handle_add_to_cluster,
        }

        # Get the handler for the message type and execute it
        handler = message_handlers.get(message.type)

        return handler(message)

    def next_timeout(self):
        """
        Returns the next timeout value based on the current time and the timeout duration.
        """
        return time.time() + self.timeout

    def send_response(self, original_message, is_accepted=True):
        """
        Constructs and sends a response message back to the sender of the original message.
        """

        response_payload = {
            "response": is_accepted, # Boolean indicating whether the request was accepted
            "current_term": self.server.current_term,
        }

        response_message = Message(
            source=self.server.id,
            destination=original_message.src,
            term=original_message.term,
            payload=response_payload,
            message_type=MessageType.ClientResponse,
        )

        # Deliver the response to the original sender
        self.server.send_message(response_message, target_id=original_message.src)

    def find_current_leader(self):
        """Find the current leader from the server's neighbors."""
        for neighbor in self.server.neighbors:
            if isinstance(neighbor.role, Leader):
                return neighbor
        return None

    def process_join_request(self, message):
        """Redirect the join request to the leader."""
        leader = self.server.get_leader()
        if leader:
            join_request_message = Message(
                message.src,
                leader.id,
                message.term,
                message.payload,
                MessageType.RequestToJoin,
                join_upon_confirmation=message.join_upon_confirmation
            )
            print(f"Server {self.server.id} is redirecting join request to leader {leader.id}")
            self.server.send_message(join_request_message, target_id=leader.id)
        else:
            print(f"Server {self.server.id} has no leader to process join request")
        return self, None

    def on_leader_timeout(self, message):
        """This is called when the leader timeout is reached."""

    def process_vote_request(self, message):
        """This is called when there is a vote request."""

    def process_vote_received(self, message):
        """This is called when this node receives a vote."""

    def process_append_entries(self, message):
        """This is called when there is a request to append an entry to the log."""

    def process_response(self, message):
        """This is called when a response is sent back to the Leader."""

    def execute_client_command(self, message):
        """This is called when a client command is received."""

    def add_server_to_cluster(self, new_server_id):
        """This is called when a new server joins the cluster."""
    
    def handle_add_to_cluster(self, message):
        """This is called when a server is added to the cluster."""


class Follower(Role):
    def __init__(self, timeout=1):
        self.prev_vote = {}
        self.timeout = timeout
        self.timeout_time = self.next_timeout()

    def process_vote_request(self, message):
        """
        Handles an incoming vote request.
        Sends a response based on the term and log consistency.
        """
        if self.should_grant_vote(message):
            self.prev_vote[message.term] = message.src
            self.send_vote_response(message, granted=True)
        else:
            self.send_vote_response(message, granted=False)
        return self, None
    
    def should_grant_vote(self, message):
        """
        Determines whether to grant a vote to the candidate.
        Checks term consistency and log matching.
        """
        return (
            message.term not in self.prev_vote and
            (message.payload["last_log_term"], message.payload["last_log_index"]) >= 
            (self.server.last_log_term, self.server.last_log_index)
        )
        
    def send_vote_response(self, message, granted):
        """Send a response to a vote request."""
        response = Message(
            self.server.id,
            message.src,
            message.term,
            {"response": granted},
            MessageType.RequestVoteResponse
        )
        self.server.send_message(response, target_id=message.src)

    def handle_resume(self):
        """
        Resume as a follower by checking for a leader
        and syncing state if necessary.
        """
        leader = self.find_current_leader()
        if leader:
            leader.role.sync_log_with_follower(self.server.id)

    def process_append_entries(self, message):
        """
        Handles an append entries request from the leader.
        Updates log entries and commit index if necessary.
        """

        # Update the commit index if the leader's commit index is greater
        if message.payload["leader_commit"] > self.server.commit_index:
            self.update_commit_index(message.payload["leader_commit"])

        self.timeout_time = self.next_timeout()

        # Check for term mismatch
        if message.term < self.server.current_term:
            self.send_response(message, is_accepted=False)
            return self, None
        self.server.current_term = message.term

        # Check if the log entries can be appended. If so, apply them. Otherwise, reject the request.
        if message.payload:
            if not self.validate_log_entries(message.payload):
                self.send_response(message, is_accepted=False)
                return self, None
            self.apply_log_entries(message)
        return self, None
        
    def update_commit_index(self, leader_commit):
        """Update the server's commit index."""
        self.server.commit_index = min(leader_commit, len(self.server.log) - 1)

    def validate_log_entries(self, payload):
        """Validate the log entries in the append entries payload."""
        log = self.server.log
        if payload["last_log_index"] <= -1:
            return True
        if payload["last_log_index"] > -1 and len(log) <= payload["last_log_index"]:
            return False
        if len(log) > 0 and log[payload["last_log_index"]]["term"] != payload["last_log_term"]:
            self.server.log = log[:payload["last_log_index"]]
            self.server.last_log_index = payload["last_log_index"]
            self.server.last_log_term = payload["last_log_term"]
            return False
        return True

    def apply_log_entries(self, message):
        """Apply log entries from the leader to the server's log."""
        log = self.server.log
        payload = message.payload
        if len(payload["entries"]) > 0:
            log.extend(payload["entries"])
            self.server.last_log_index = len(log) - 1
            self.server.last_log_term = log[-1]["term"]
            self.server.log = log
        
    def execute_client_command(self, message):
        return self, None


class Candidate(Role):
    def __init__(self):
        self.prev_vote = {}

    def assign_to_server(self, server):
        self.server = server
        self.votes = {}
        self.start_election()

    def process_vote_request(self, message):
        return self, None
    
    def send_vote_response(self, message, granted):
        """Send a response to a vote request."""
        response = Message(
            self.server.id,
            message.src,
            message.term,
            {"response": granted},
            MessageType.RequestVoteResponse
        )
        self.server.send_message(response, target_id=message.src)

    def process_vote_received(self, message):
        """
        Handles a vote received during an election.
        If the majority is achieved, transitions to leader state.
        """
        self.record_vote(message.src, message)

        if self.has_majority():
            return self.promote_to_leader()
        return self, None

    def record_vote(self, voter_id, message):
        """Records a vote from a voter."""
        self.votes[voter_id] = message

    def has_majority(self):
        """Checks if the candidate has received the majority of votes."""
        return len(self.votes) >= (self.server.total_nodes // 2) + 1

    def promote_to_leader(self):
        """
        Promotes the candidate to a leader role.
        Updates neighbors to follower roles.
        """
        leader = Leader()
        leader.assign_to_server(self.server)
        print(f"Server {self.server.id} has been elected leader")
        self.server.server_state = ServerState.LEADER

        # Iterate over neighbors and update their roles to followers
        for neighbor in self.server.neighbors:
            if neighbor.server_state != ServerState.DEAD and neighbor.server_state != ServerState.JOINING:
                neighbor.server_state = ServerState.FOLLOWER
                neighbor.role = Follower()
                neighbor.role.assign_to_server(neighbor)

        return leader, None

    def start_election(self):
        """
        Starts a new election by incrementing the term,
        resetting votes, and broadcasting a vote request.
        """
        self.prepare_for_election()
        self.broadcast_vote_request()

    def prepare_for_election(self):
        """Prepares the candidate for a new election cycle."""
        print(f"Server {self.server.id} is starting election")
        self.timeout_time = time.time() + randint(1, 2) # Change based on number of nodes and network latency
        self.votes = {}
        self.server.current_term += 1

    def broadcast_vote_request(self):
        """Broadcasts a vote request to all neighbors."""
        election_message = self.create_vote_request_message()
        self.server.send_message(election_message)
        self.prev_vote = self.server.id

    def create_vote_request_message(self):
        """Creates the vote request message."""
        return Message(
            self.server.id,
            None,
            self.server.current_term,
            {
                "last_log_index": self.server.last_log_index,
                "last_log_term": self.server.last_log_term,
            },
            MessageType.RequestVote,
        )

class Leader(Role):
    def __init__(self, timeout=0.25):
        self.timeout = timeout                                  # Timeout for leader heartbeat
        self.timeout_time = self.next_timeout()                 # Time for next heartbeat
        self.match_indexes = defaultdict(int)                   # Match index for each follower
        self.next_indexes = defaultdict(int)                    # Next log index for each follower
        self.ack_counts = defaultdict(int)                      # Acknowledgement count for each log entry
        self.pending_messages = {}                              # Pending messages for each follower

    def assign_to_server(self, server):
        """Initialize leader-specific configurations for the server."""
        self.server = server
        self.initialize_log_indexes()

    def initialize_log_indexes(self):
        """Sets up the next and match indexes for all followers."""
        for neighbor in self.server.neighbors:
            self.next_indexes[neighbor.id] = self.server.last_log_index + 1
            self.match_indexes[neighbor.id] = 0

    def sync_log_with_follower(self, follower_id, join_upon_confirmation=False):
        """Synchronize logs with a specific follower."""
        if not self.server.log:
            return  # No log to synchronize
        
        if join_upon_confirmation:
            print(f"Leader Server {self.server.id} is syncing log with Server {follower_id}. Log size of {len(self.server.log)}")

        previous_index, current_entries = self.get_log_entries_for_sync(follower_id)
        message = self.create_append_entries_message(follower_id, previous_index, current_entries)
        message.join_upon_confirmation = join_upon_confirmation
        self.server.send_message(message, target_id=follower_id)

    def get_log_entries_for_sync(self, follower_id):
        """Retrieve log entries and their previous index for syncing."""
        previous_index = self.next_indexes[follower_id] - 1
        previous_term = self.server.log[previous_index]["term"] if previous_index >= 0 else None
        entries = self.server.log[self.next_indexes[follower_id]:]
        return previous_index, entries

    def create_append_entries_message(self, follower_id, previous_index, entries):
        """Creates an AppendEntries message."""
        return Message(
            self.server.id,
            follower_id,
            self.server.current_term,
            {
                "leader_id": self.server.id,
                "last_log_index": previous_index,
                "last_log_term": self.server.log[previous_index]["term"] if previous_index >= 0 else None,
                "entries": entries,
                "leader_commit": self.server.commit_index,
            },
            MessageType.AppendEntries,
        )

    def execute_client_command(self, message):
        """Processes a client command and initiates log replication."""
        log_entry = self.create_log_entry(message.payload["command"])
        self.update_server_log(log_entry)
        self.notify_followers(log_entry)
        return self, None

    def create_log_entry(self, command):
        """
        Creates a new log entry based on the client command.

        :param command: The command payload from the client.
        :return: A dictionary representing the log entry.
        """
        return {"term": self.server.current_term, "value": command}

    def update_server_log(self, log_entry):
        """
        Updates the server's log and metadata after appending a new log entry.

        :param log_entry: The log entry to append.
        """
        self.server.last_log_index = len(self.server.log)
        self.server.last_log_term = log_entry["term"]
        self.server.log.append(log_entry)

    def notify_followers(self, log_entry):
        """
        Sends the new log entry to all followers for replication.

        :param log_entry: The log entry to replicate.
        """
        for neighbor in self.server.neighbors:
            self.pending_messages[neighbor.id] = 1

        append_message = Message(
            self.server.id,
            None,
            self.server.current_term,
            {
                "leader_id": self.server.id,
                "last_log_index": self.server.last_log_index,
                "last_log_term": self.server.last_log_term,
                "entries": [log_entry],
                "leader_commit": self.server.commit_index,
            },
            MessageType.AppendEntries
        )
        self.server.send_message(append_message)

    def send_heartbeat(self):
        """Sends periodic heartbeat messages to maintain leadership."""
        self.timeout_time = self.next_timeout()
        heartbeat_message = self.create_heartbeat_message()
        self.broadcast_message_to_all(heartbeat_message)

    def create_heartbeat_message(self):
        """Creates a heartbeat message."""
        return Message(
            self.server.id,
            None,
            self.server.current_term,
            {
                "leader_id": self.server.id,
                "last_log_index": self.server.last_log_index,
                "last_log_term": self.server.last_log_term,
                "entries": [],
                "leader_commit": self.server.commit_index,
            },
            MessageType.AppendEntries,
        )

    def broadcast_message_to_all(self, message):
        """Broadcasts a message to all followers."""
        for neighbor in self.server.neighbors:
            self.server.send_message(message, target_id=neighbor.id)

    def process_response(self, response_message):
        """Processes a response from a follower."""
        if not response_message.payload["response"]:
            self.retry_log_sync(response_message)
        else:
            self.update_acknowledgements(response_message)

    def retry_log_sync(self, response_message):
        """Retries log synchronization if the follower rejected the entries."""
        follower_id = response_message.src
        self.next_indexes[follower_id] -= 1
        self.sync_log_with_follower(follower_id)

    def update_acknowledgements(self, response_message):
        """Updates acknowledgments for log replication and commits if quorum is reached."""
        follower_id = response_message.src
        self.advance_next_index(follower_id)
        self.update_commit_index()

    def advance_next_index(self, follower_id):
        """Advances the next index for a follower after successful replication."""
        num_messages = len(self.pending_messages[follower_id])
        self.next_indexes[follower_id] += num_messages

    def update_commit_index(self):
        """Commits log entries if a majority of followers acknowledge."""
        for log_index in range(self.server.commit_index + 1, self.server.last_log_index + 1):
            if self.is_majority_acknowledged(log_index):
                self.server.commit_index = log_index

    def is_majority_acknowledged(self, log_index):
        """Checks if a log entry is acknowledged by the majority."""
        acknowledgment_count = sum(1 for follower in self.server.neighbors
                                    if self.match_indexes[follower.id] >= log_index)
        return acknowledgment_count > len(self.server.neighbors) // 2

    def calculate_next_timeout(self):
        """Calculates the next timeout for the leader."""
        return time.time() + self.timeout
    
    def process_join_request(self, message):
        """Process a join request from a new server."""
        if message.src in self.next_indexes:
            print(f"Leader Server {self.server.id}: Server {message.src} is already in the cluster.")
            return self, None
        
        print(f"Leader Server {self.server.id} received join request from {message.src}")
        self.sync_log_with_follower(message.src, join_upon_confirmation=True)
        return self, None

    def handle_add_to_cluster(self, message):
        """Add a new server to the cluster."""
        new_server_id = message.src
        print(f"Leader Server {self.server.id} has added Server {new_server_id} to the cluster")

class Joining(Role):
    def __init__(self, timeout=1):
        self.timeout = timeout
        self.timeout_time = self.next_timeout()

    def process_append_entries(self, message):
        """
        Handles an append entries request from the leader.
        Updates log entries and commit index if necessary.
        """

        # Update the commit index if the leader's commit index is greater
        if message.payload["leader_commit"] > self.server.commit_index:
            self.update_commit_index(message.payload["leader_commit"])

        self.timeout_time = self.next_timeout()

        # Check for term mismatch
        if message.term < self.server.current_term:
            self.send_response(message, is_accepted=False)
            return self, None
        self.server.current_term = message.term

        # Check if the log entries can be appended. If so, apply them. Otherwise, reject the request.
        if message.payload:
            if not self.validate_log_entries(message.payload):
                self.send_response(message, is_accepted=False)
                return self, None
            self.apply_log_entries(message)
            if message.join_upon_confirmation:
                self.transition_to_follower()
                self.send_add_to_cluster()
        return self, None
    
    def update_commit_index(self, leader_commit):
        """Update the server's commit index."""
        self.server.commit_index = min(leader_commit, len(self.server.log) - 1)

    def validate_log_entries(self, payload):
        """Validate the log entries in the append entries payload."""
        log = self.server.log
        if payload["last_log_index"] <= -1:
            return True
        if payload["last_log_index"] > -1 and len(log) <= payload["last_log_index"]:
            return False
        if len(log) > 0 and log[payload["last_log_index"]]["term"] != payload["last_log_term"]:
            self.server.log = log[:payload["last_log_index"]]
            self.server.last_log_index = payload["last_log_index"]
            self.server.last_log_term = payload["last_log_term"]
            return False
        return True

    def apply_log_entries(self, message):
        """Apply log entries from the leader to the server's log."""
        log = self.server.log
        payload = message.payload
        if len(payload["entries"]) > 0:
            log.extend(payload["entries"])
            self.server.last_log_index = len(log) - 1
            self.server.last_log_term = log[-1]["term"]
            self.server.log = log

    def transition_to_follower(self):
        """Transition to the follower state."""
        self.server.server_state = ServerState.FOLLOWER
        self.server.role = Follower()
        self.server.role.assign_to_server(self.server)
        print(f"Server {self.server.id} has transitioned to Follower state. It has state {self.server.server_state}")

    def send_add_to_cluster(self):
        """Send a message to the leader to add the server to the cluster."""
        print(f"Server {self.server.id} is joining the cluster")
        leader = self.find_current_leader()
        if leader:
            message = Message(
                self.server.id,
                leader.id,
                self.server.current_term,
                {},
                MessageType.AddToCluster
            )
            self.server.send_message(message, target_id=leader.id)
        else:
            print(f"Server {self.server.id} has no leader to process join request... retrying in 2 seconds")
            time.sleep(2)
            self.send_add_to_cluster()