from state import State
from message import Message

class Voter(State):
    def __init__(self):
        """
        Initializes the Voter state, tracking the last vote cast per term.
        """
        super().__init__()
        self._last_vote = {}

    def on_vote_request(self, message):
        """
        Handles a vote request message.

        :param message: The vote request message
        :return: Tuple (self, None) to maintain the current state
        """
        print(f"Server {self._server._name} received vote request from {message.sender}")
        
        # Check if this server has already voted in the given term
        has_not_voted = message._term not in self._last_vote

        # Check if the candidate's log is at least as up-to-date as this server's log
        log_is_up_to_date = (message._data["lastLogTerm"], message._data["lastLogIndex"]) >= (
            self._server._lastLogTerm, self._server._lastLogIndex
        )

        if has_not_voted and log_is_up_to_date:
            # Grant vote
            self._last_vote[message._term] = message.sender
            self._send_vote_response_message(message)
        else:
            # Deny vote
            self._send_vote_response_message(message, yes=False)

        return self, None

    def _send_vote_response_message(self, msg, yes=True):
        """
        Sends a vote response message back to the requestor.

        :param msg: The original vote request message
        :param yes: Whether the vote is granted or denied
        """
        vote_response = Message(
            sender=self._server._name,
            receiver=msg.sender,
            term=msg.term,
            data={"response": yes},
            message_type=Message.RequestVoteResponse
        )
        self._server.send_message_response(vote_response)

