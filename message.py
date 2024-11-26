import time
from enum import Enum, auto

# Enum for the different types of messages that can be sent between servers
class MessageType(Enum):
    AppendEntries = auto()
    RequestVote = auto()
    RequestVoteResponse = auto()
    ClientResponse = auto()
    ClientCommand = auto()
    RequestToJoin = auto()
    AddToCluster = auto()

class Message:
    def __init__(self, source, destination, term, payload, message_type, unpack_time=None, join_upon_confirmation=False):
        self.timestamp = time.time()     # The time at which the message was created
        self.src = source                # The name of the server that sent the message
        self.dst = destination           # The name of the server that should receive the message
        self.payload = payload           # The data that the message carries
        self.term = term                 # The Raft term in which the message was sent 
        self.type = message_type         # The type of message that was sent (i.e. AppendEntries, RequestVote, etc.)
        self.unpack_time = unpack_time if unpack_time else self.timestamp # The time at which the message can be unpacked and read
        self.join_upon_confirmation = join_upon_confirmation # A flag to indicate that the server should join the cluster upon receiving a confirmation

    def __lt__(self, other):
        """
        A message is considered 'less than' another if its `unpack_time` is earlier. Used for priority queue ordering.
        """
        return self.unpack_time < other.unpack_time

    def __eq__(self, other):
        """
        Equality based on unpack_time. Used for popping from the priority queue.
        """
        return self.unpack_time == other.unpack_time and self.term == other.term

    def set_unpack_time(self, unpack_time):
        """
        Updates the unpack time of the message. This is used to simulate network delays (see usage in raft_server.py).

        :param unpack_time: The new time at which the message can be unpacked and read
        """
        self.unpack_time = unpack_time

    def __hash__(self):
        return hash((self.src, self.dst, self.term, self.type, self.timestamp))