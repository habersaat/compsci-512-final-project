import time
from random import randint
from variables import *
from voter import Voter
from leader import Leader
from follower import Follower
from message import Message


class Candidate(Voter):
    """
    Represents the Candidate state in the Raft protocol.
    Extends the Voter state to participate in leader elections.
    """

    def set_server(self, server):
        """
        Associates this Candidate state with a server and starts the election process.

        :param server: The server instance
        """
        self._server = server
        self._votes = {}
        self._start_election()

    # ----------------------- Message Handlers -----------------------

    def on_vote_request(self, message):
        """
        Handles incoming vote requests while in Candidate state.
        Candidates cannot grant votes.

        :param message: The vote request message
        :return: Tuple (self, None) to maintain the current state
        """
        return self, None

    def on_vote_received(self, message):
        """
        Handles incoming vote responses. If the Candidate wins the election, it transitions to Leader state.

        :param message: The vote received message
        :return: Tuple (new_state, None) indicating state transition or no change
        """
        self._votes[message.sender] = message
        total_votes = len(self._votes)

        # Check if the Candidate has received majority votes
        if total_votes >= (self._server._total_nodes - 1) // 2 + 1:
            print(f"Server {self._server._name} has been elected leader")
            return self._transition_to_leader()

        return self, None

    # ----------------------- Election Process -----------------------

    def _start_election(self):
        """
        Initiates the election process by sending RequestVote messages to all neighbors.
        """
        print(f"{self._server._name} is starting an election")
        self._timeoutTime = time.time() + randint(5, 6)  # Election timeout in seconds
        self._votes = {}  # Reset votes for the new term
        self._server._currentTerm += 1  # Increment the term

        # Create and send the election message
        election_message = Message(
            sender=self._server._name,
            receiver=None,  # Broadcast
            term=self._server._currentTerm,
            data={
                "lastLogIndex": self._server._lastLogIndex,
                "lastLogTerm": self._server._lastLogTerm,
            },
            message_type=Message.RequestVote
        )
        self._server.send_message(election_message)

        # Record that this server has voted for itself
        self._last_vote = self._server._name

    def _transition_to_leader(self):
        """
        Transitions the Candidate to the Leader state and informs neighbors of the new leader.

        :return: The Leader state instance
        """
        leader = Leader()
        leader.set_server(self._server)
        self._server._serverState = leaderState

        # Notify all neighbors of the state change
        for neighbor in self._server._neighbors:
            if neighbor._serverState != deadState:
                neighbor._serverState = followerState
                neighbor._state = Follower()
                neighbor._state.set_server(neighbor)

        return leader, None
