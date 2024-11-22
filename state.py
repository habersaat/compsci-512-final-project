import time
from message import Message


class State:
    def __init__(self):
        self._server = None
        self._currentTime = None
        self._timeout = None

    # ----------------------- Server Management -----------------------

    def set_server(self, server):
        """
        Associates this state with a server instance.

        :param server: The server instance
        """
        self._server = server

    # ----------------------- Message Handling -----------------------

    def on_message(self, message):
        """
        Processes incoming messages and routes them to the appropriate handler.

        :param message: The incoming message
        :return: Tuple (new_state, response) or None
        """
        # Update term if the message term is higher
        if message.term > self._server._currentTerm:
            self._server._currentTerm = message.term
        elif message.term < self._server._currentTerm:
            self._send_response_message(message, yes=False)
            return self, None

        # Route the message to the appropriate handler
        message_handlers = {
            Message.AppendEntries: self.on_append_entries,
            Message.RequestVote: self.on_vote_request,
            Message.RequestVoteResponse: self.on_vote_received,
            Message.Response: self.on_response_received,
            Message.ClientCommand: self.run_client_command,
        }

        handler = message_handlers.get(message.type)
        if handler:
            return handler(message)
        return self, None

    # ----------------------- Message Handlers -----------------------

    def on_leader_timeout(self, message):
        """
        Called when the leader timeout is reached.
        Override in subclasses for specific behavior.
        """
        pass

    def on_vote_request(self, message):
        """
        Called when a vote request is received.
        Override in subclasses for specific behavior.

        :param message: The vote request message
        """
        pass

    def on_vote_received(self, message):
        """
        Called when a vote is received.
        Override in subclasses for specific behavior.

        :param message: The vote received message
        """
        pass

    def on_append_entries(self, message):
        """
        Called when an append entries request is received.
        Override in subclasses for specific behavior.

        :param message: The append entries message
        """
        pass

    def on_response_received(self, message):
        """
        Called when a response is sent back to the leader.
        Override in subclasses for specific behavior.

        :param message: The response message
        """
        pass

    def run_client_command(self, message):
        """
        Called when a client command is received.
        Override in subclasses for specific behavior.

        :param message: The client command message
        """
        pass

    # ----------------------- Utility Methods -----------------------

    def _nextTimeout(self):
        """
        Calculates the next timeout based on the current time and the timeout interval.

        :return: The next timeout timestamp
        """
        self._currentTime = time.time()
        return self._currentTime + self._timeout

    def _send_response_message(self, msg, yes=True):
        """
        Sends a response message back to the sender.

        :param msg: The original message
        :param yes: Whether the response is affirmative or not
        """
        response = Message(
            sender=self._server._name,
            receiver=msg.sender,
            term=msg.term,
            data={
                "response": yes,
                "currentTerm": self._server._currentTerm,
            },
            message_type=Message.Response
        )
        self._server.send_message_response(response)
