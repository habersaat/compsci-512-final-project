import time


class Message:
    """
    Represents a message used in the Raft protocol with various message types.
    """

    # Message Types
    AppendEntries = 0
    RequestVote = 1
    RequestVoteResponse = 2
    Response = 3
    ClientCommand = 4

    def __init__(self, sender, receiver, term, data, message_type, unpack_time=None):
        """
        Initializes a Message instance.

        :param sender: The name of the sender server
        :param receiver: The name of the receiver server
        :param term: The term number associated with the message
        :param data: The payload/data of the message
        :param message_type: The type of the message
        """
        self._timestamp = int(time.time())  # Record the message creation time
        self._sender = sender
        self._receiver = receiver
        self._data = data
        self._term = term
        self._type = message_type
        self._unpack_time = unpack_time if unpack_time is not None else time.time()

    # ----------------------- Properties -----------------------

    @property
    def receiver(self):
        """
        :return: The name of the receiver server.
        """
        return self._receiver

    @property
    def sender(self):
        """
        :return: The name of the sender server.
        """
        return self._sender

    @property
    def data(self):
        """
        :return: The payload or data of the message.
        """
        return self._data

    @property
    def timestamp(self):
        """
        :return: The creation timestamp of the message.
        """
        return self._timestamp

    @property
    def term(self):
        """
        :return: The term number associated with the message.
        """
        return self._term

    @property
    def type(self):
        """
        :return: The type of the message (e.g., RequestVote, AppendEntries).
        """
        return self._type
    
    @property
    def unpack_time(self):
        """
        :return: The time at which the message can be unpacked and read.
        """
        return self._unpack_time
    
    # ----------------------- Setter Method -----------------------

    def set_unpack_time(self, unpack_time):
        """
        Updates the unpack time of the message.

        :param unpack_time: The new time at which the message can be unpacked and read
        """
        self._unpack_time = unpack_time

    # ----------------------- Utility Methods -----------------------

    def is_type(self, message_type):
        """
        Checks if the message is of a specific type.

        :param message_type: The message type to check against
        :return: True if the message matches the type, False otherwise
        """
        return self._type == message_type

    def to_dict(self):
        """
        Converts the message into a dictionary format.

        :return: A dictionary representing the message
        """
        return {
            "timestamp": self._timestamp,
            "sender": self._sender,
            "receiver": self._receiver,
            "data": self._data,
            "term": self._term,
            "type": self._type,
        }

    def __str__(self):
        """
        Provides a human-readable string representation of the message.

        :return: A string describing the message
        """
        return (
            f"Message(sender={self._sender}, receiver={self._receiver}, "
            f"term={self._term}, type={self._type}, data={self._data}, "
            f"unpack_time={self._unpack_time})"
        )
