from state import State
from message import Message
from collections import defaultdict


class Leader(State):
    """
    Represents the Leader state in the Raft protocol.
    Responsible for log replication, handling client commands, and maintaining consistency.
    """

    def __init__(self, timeout=0.5):
        """
        Initializes the Leader state.

        :param timeout: Heartbeat timeout in seconds
        """
        super().__init__()
        self._timeout = timeout
        self._nextIndexes = defaultdict(int)  # Next log index to send to each follower
        self._matchIndex = defaultdict(int)  # Highest log index replicated on each follower
        self._ackCount = defaultdict(int)  # Acknowledgement count for each log index
        self._timeoutTime = self._nextTimeout()
        self._numofMessages = {}  # Tracks the number of entries sent to each follower


    def set_server(self, server):
        """
        Associates this Leader state with a server and initializes replication indices.

        :param server: The server instance
        """
        self._server = server
        for neighbor in self._server._neighbors:
            self._nextIndexes[neighbor._name] = max(self._server._lastLogIndex + 1, 0)
            self._matchIndex[neighbor._name] = 0
        print(f"Leader initialized with nextIndexes: {self._nextIndexes}")

    def send_pending_messages(self, server_name):
        log = self._server._log
        if not log:
            return
        
        self._nextIndexes[server_name] = len(log) - 1
        prevLogTerm = log[-2]["term"] if len(log) > 1 else None

        append_entries_message = Message(
            sender=self._server._name,
            receiver=server_name,
            term=self._server._currentTerm,
            data={
                "leaderId": self._server._name,
                "prevLogIndex": len(log) - 2,
                "prevLogTerm": prevLogTerm,
                "entries": [log[-1]],
                "leaderCommit": self._server._commitIndex,
            },
            message_type=Message.AppendEntries
        )
        self._server.send_message_response(append_entries_message)

    def run_client_command(self, message):
        """
        Handles a client command by appending it to the log and replicating it to followers.

        :param message: The client command message
        :return: Tuple (self, None) to maintain the current state
        """
        # print("running client command")
        term = self._server._currentTerm
        value = message._data["command"]
        log_entry = {"term": term, "value": value}

        self._server._lastLogIndex = len(self._server._log) - 1
        self._server._lastLogTerm = term

        if self._server._lastLogIndex > -1:
            self._server._lastLogTerm = self._server._log[self._server._lastLogIndex]["term"]

        self._server._log.append(log_entry)
        for n in self._server._neighbors:
            self._numofMessages[n._name] = 1

        append_entries_message = Message(
            sender=self._server._name,
            receiver=None,
            term=self._server._currentTerm,
            data={
                "leaderId": self._server._name,
                "prevLogIndex": self._server._lastLogIndex,
                "prevLogTerm": self._server._lastLogTerm,
                "entries": [log_entry],
                "leaderCommit": self._server._commitIndex,
            },
            message_type=Message.AppendEntries
        )

        self._server.send_message(append_entries_message)
        return self, None

    def on_response_received(self, message):
        """x
        Handles responses to AppendEntries messages.

        :param message: The response message
        :return: Tuple (self, None) to maintain the current state
        """
        if not message.data["response"]:
            self._handle_append_failure(message)
        else:
            self._handle_append_success(message)

        return self, None

    def _handle_append_failure(self, message):
        """
        Handles a failed AppendEntries response by decrementing the nextIndex for the follower 
        and resending the appropriate log entries.

        :param message: The response message indicating failure
        """

        if self._nextIndexes[message.sender] == 0:
            previous_index = -1
            previous_log_term = None
            current_entries = self._server._log

            append_entry_message = Message(
                sender=self._server._name,
                receiver=message.sender,
                term=self._server._currentTerm,
                data={
                    "leaderId": self._server._name,
                    "prevLogIndex": previous_index,
                    "prevLogTerm": previous_log_term,
                    "entries": current_entries,
                    "leaderCommit": self._server._commitIndex,
                },
                message_type=Message.AppendEntries
            )
        else:
            # Decrement the nextIndex for the sender to backtrack the log.
            self._nextIndexes[message.sender] -= 1

            # Calculate the previous index and prepare the log entries to resend
            previous_index = self._nextIndexes[message.sender] - 1


            previous_entry = self._server._log[previous_index]  # Entry before the failed log
            current_entries = self._server._log[self._nextIndexes[message.sender]:]  # Entries to resend

            # Update the number of log entries being sent for this follower
            self._numofMessages[message.sender] = len(current_entries)

            # Create a new AppendEntries message with the adjusted log data
            append_entry_message = Message(
                sender=self._server._name,
                receiver=message.sender,
                term=self._server._currentTerm,
                data={
                    "leaderId": self._server._name,
                    "prevLogIndex": previous_index,
                    "prevLogTerm": previous_entry["term"],
                    "entries": current_entries,
                    "leaderCommit": self._server._commitIndex,
                },
                message_type=Message.AppendEntries
            )

        print(f"Leader resending log entry {previous_index + 1} to {message.sender}")

        # Send the updated AppendEntries message to the follower
        self._server.send_message_response(append_entry_message)

    def _handle_append_success(self, message):
        """
        Handles a successful AppendEntries response by updating indices and committing entries.

        :param message: The response message indicating success
        """
        # Update the nextIndex for the follower based on the number of entries acknowledged
        current_index = self._nextIndexes[message.sender]
        last_index = current_index + self._numofMessages[message.sender] - 1
        self._nextIndexes[message.sender] += self._numofMessages[message.sender]

        # Iterate over all acknowledged entries from last_index to current_index
        for i in range(last_index, current_index - 1, -1):
            # Increment the acknowledgment count for the current log entry
            self._ackCount[i] += 1
            print(f"Leader received ACK for log entry {i}")

            # Check if the current log entry has been acknowledged by the majority
            if self._ackCount[i] == (self._server._total_nodes + 1) / 2:
                if i > self._server._commitIndex:
                    for j in range(self._server._commitIndex + 1, i + 1):
                        self._server._x += int(self._server._log[j]["value"])
                    self._server._commitIndex = i
                    print(f"Leader committed entries up to {self._server._commitIndex}")
                break

    def _send_heart_beat(self):
        """
        Sends a heartbeat (AppendEntries with no entries) to all followers.
        """
        self._timeoutTime = self._nextTimeout()
        self._server._lastLogIndex = len(self._server._log) - 1
        self._server._lastLogTerm = self._server._currentTerm
        if self._server._log:
            self._server._lastLogTerm = self._server._log[-1]["term"]

        heartbeat_message = Message(
            sender=self._server._name,
            receiver=None,
            term=self._server._currentTerm,
            data={
                "leaderId": self._server._name,
                "prevLogIndex": self._server._lastLogIndex,
                "prevLogTerm": self._server._lastLogTerm,
                "entries": [],
                "leaderCommit": self._server._commitIndex,
            },
            message_type=Message.AppendEntries
        )
        self._server.send_message(heartbeat_message)