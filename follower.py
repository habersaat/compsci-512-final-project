from voter import Voter
from leader import Leader


class Follower(Voter):
    """
    Represents the Follower state in the Raft protocol.
    Extends the Voter state with follower-specific behaviors.
    """

    def __init__(self, timeout=1):
        """
        Initializes the Follower state with a timeout.

        :param timeout: The timeout duration in seconds for follower inactivity
        """
        super().__init__()
        self._timeout = timeout
        self._timeoutTime = self._nextTimeout()

    # ----------------------- State Transition Handling -----------------------

    def on_resume(self):
        """
        Handles resumption of the Follower state, checking for the leader and syncing any pending messages.
        """
        leader = self._find_leader()
        if leader is not None:
            leader._state.send_pending_messages(self._server._name)

    def on_append_entries(self, message):
        """
        Handles AppendEntries RPC messages sent by the Leader.

        :param message: The AppendEntries message
        :return: Tuple (self, None) to maintain the current state
        """
        # Update committed entries in the state machine
        self._apply_committed_entries(message)

        # Reset the follower's timeout
        self._timeoutTime = self._nextTimeout()

        # Reject if the message term is stale
        if message._term < self._server._currentTerm:
            print(f"Server {self._server._name} rejected stale AppendEntries message")
            self._send_response_message(message, yes=False)
            return self, None

        # Update the server's term
        self._server._currentTerm = message._term

        # Handle log consistency and appending new entries
        if message._data:
            return self._handle_log_consistency(message)
        else:
            return self, None

    # ----------------------- Helper Methods -----------------------

    def _find_leader(self):
        """
        Finds the leader among the server's neighbors.

        :return: The leader server instance or None if no leader is found
        """
        for neighbor in self._server._neighbors:
            if isinstance(neighbor._state, Leader):
                return neighbor
        return None

    def _apply_committed_entries(self, message):
        """
        Applies committed log entries to the state machine.

        :param message: The AppendEntries message
        """
        log = self._server._log
        for index in range(self._server._commitIndex + 1, 
                           min(message._data["leaderCommit"] + 1, len(log))):
            self._server._x += int(log[index]["value"])

        # Update the commit index
        if message._data["leaderCommit"] > self._server._commitIndex:
            self._server._commitIndex = min(message._data["leaderCommit"], len(log) - 1)
            print(f"State machine of {self._server._name}: {self._server._x}")

    def _handle_log_consistency(self, message):
        """
        Ensures log consistency with the Leader and appends new entries if valid.

        :param message: The AppendEntries message
        :return: Tuple (self, None) to maintain the current state
        """
        log = self._server._log
        data = message._data

        # Check for missing logs
        if data["prevLogIndex"] > -1 and len(log) <= data["prevLogIndex"]:
            print(f"Server {self._server._name} has missing logs. Log is {log} and prevLogIndex is {data['prevLogIndex']}")
            self._send_response_message(message, yes=False)
            return self, None

        # Check for log inconsistency and truncate if necessary
        if len(log) > 0 and log[data["prevLogIndex"]]["term"] != data["prevLogTerm"]:
            log = log[:data["prevLogIndex"]]
            print(f"Server {self._server._name} truncated log: {log}")
            self._send_response_message(message, yes=False)
            self._update_server_log(log, data["prevLogIndex"], data["prevLogTerm"])
            return self, None

        # Append new entries if consistent
        if len(data["entries"]) > 0:
            self._append_entries(log, data["entries"])
            self._send_response_message(message)

        return self, None

    def _update_server_log(self, log, last_log_index, last_log_term):
        """
        Updates the server's log after truncation.

        :param log: The truncated log
        :param last_log_index: The index of the last valid log entry
        :param last_log_term: The term of the last valid log entry
        """
        self._server._log = log
        self._server._lastLogIndex = last_log_index
        self._server._lastLogTerm = last_log_term

    def _append_entries(self, log, entries):
        """
        Appends new entries to the log and updates the server's log state.

        :param log: The current log
        :param entries: The new entries to append
        """
        log.extend(entries)
        # print(f"Server {self._server._name} updated log: {log}")

        self._server._lastLogIndex = len(log) - 1
        self._server._lastLogTerm = log[-1]["term"]
        self._server._log = log
