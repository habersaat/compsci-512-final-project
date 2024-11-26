import time
from threading import Thread, Lock
from random import randint
from server_roles import Leader, Follower, Candidate, Joining, ServerState
from raft_server import Server
from message import Message, MessageType
import random
import logging

class Cluster:
    term_counter = 0
    config = {}
    next_server_id = 0
    lock = Lock()
    all_nodes = set()
    committed_messages = set()
    max_commit_index = 0


def generate_random_ip():
    """
    Generates a random IP address.
    """
    return ".".join(str(randint(0, 255)) for _ in range(4))
    
def process_messages_for_server(server):
    """
    Processes all pending messages in a server's queue.
    """
    while True:
        message = server.process_message()
        if not message:
            break
        server.handle_message(message)


def monitor_all_servers():
    """
    Monitors all servers and processes their messages periodically.
    """
    while True:
        time.sleep(0.001)
        with Cluster.lock:
            cluster_config = list(Cluster.config.items())
        for server_id, server_data in cluster_config:
            server = server_data["instance"]
            if server.server_state != ServerState.DEAD:
                process_messages_for_server(server)


def leader_behavior(server):
    """
    Handles leader-specific tasks.
    """
    HEARTBEAT_TIMEOUT = 2.5
    if time.time() >= server.role.timeout_time:
        server.role.send_heartbeat()
    
    # Update active nodes
    active_nodes = set()
    for node, last_heartbeat in server.node_activity.items():
        if time.time() - last_heartbeat < HEARTBEAT_TIMEOUT:
            active_nodes.add(node)
    server.active_nodes = active_nodes

    # Add committed messages to the set
    for i in range(len(server.log)):
        if i >= server.commit_index:
            break
        Cluster.committed_messages.add(str(server.log[i]))


def candidate_behavior(server):
    """
    Handles candidate-specific tasks.
    """
    if time.time() >= server.role.timeout_time:
        server.role = Follower()
        server.role.assign_to_server(server)
        print(f"Server {server.id} transitioned back to Follower after timeout.")


def follower_behavior(server):
    """
    Handles follower-specific tasks.
    """
    if Cluster.term_counter >= 1 and time.time() >= server.role.timeout_time:
        print(f"Server {server.id} timed out waiting for heartbeat. Starting election...")
        server.server_state = ServerState.CANDIDATE


def promote_to_candidate(server):
    """
    Converts a follower to a candidate and starts an election.
    """
    time.sleep(2) # Change based on number of servers or network conditions
    if server.server_state == ServerState.CANDIDATE:
        server.role = Candidate()
        server.role.assign_to_server(server)


def manage_server_lifecycle(server_id):
    """
    Manages the lifecycle and role-specific tasks of a server.
    """
    with Cluster.lock:
        server = Cluster.config[server_id]["instance"]

    while isinstance(server.role, Joining):
        print(f"Server {server_id} is waiting to join the cluster.")
        time.sleep(1)  # Simulate a delay before joining the cluster
    initialize_server(server, server_id)

    while True:
        # Handle server behavior based on role
        if isinstance(server.role, Leader):
            leader_behavior(server)
        elif isinstance(server.role, Candidate):
            candidate_behavior(server)
        elif isinstance(server.role, Follower):
            follower_behavior(server)

        # Handle transitions and server state changes
        if server.server_state == ServerState.DEAD:
            shut_down_server(server, server_id)
            break
        if server.server_state == ServerState.CANDIDATE and not isinstance(server.role, Candidate):
            promote_to_candidate(server)

        time.sleep(0.001) # Avoid tight polling


def initialize_server(server, server_id):
    """
    Initializes or resumes a server.
    """
    if server.server_state == ServerState.FOLLOWER:
        print(f"Server {server_id} initialized as Follower.")
    elif server.server_state == ServerState.RESUME:
        print(f"Server {server_id} resuming as Follower.")
        server.role = Follower()
        server.role.assign_to_server(server)
        server.role.handle_resume()
        server.server_state = ServerState.FOLLOWER
    elif server.server_state == ServerState.JOINING:
        Cluster.config[server_id]["instance"].server_state = ServerState.FOLLOWER


def shut_down_server(server, server_id):
    """
    Terminates a server's operations.
    """
    print(f"Server {server_id} has been shut down.")
    server.role = Follower()
    server.role.assign_to_server(server)


def add_server_to_cluster():
    """
    Adds a new server to the cluster and connects it to peers.
    """
    ip = generate_random_ip()
    new_server_role = Follower()
    new_server = Server(Cluster.next_server_id, new_server_role, ip, set(), [])
    new_server.total_nodes = Cluster.next_server_id + 1

    with Cluster.lock:
        cluster_config = list(Cluster.config.items())

    # Connect the new server to existing ones
    for existing_server_id, data in cluster_config:
        existing_server = data["instance"]
        existing_server.neighbors.append(new_server)
        new_server.neighbors.append(existing_server)
        existing_server.total_nodes += 1

    Cluster.config[Cluster.next_server_id] = {"instance": new_server}
    Thread(target=manage_server_lifecycle, args=(Cluster.next_server_id,), daemon=True).start()
    Cluster.next_server_id += 1

    Cluster.all_nodes.add(new_server.id)

def add_request_to_join_cluster():
    """
    Adds a request to join the cluster.
    """
    print("Adding a request to join the cluster...")
    ip = generate_random_ip()
    new_server_role = Joining()
    new_server = Server(Cluster.next_server_id, new_server_role, ip, set(), [])
    new_server.total_nodes = Cluster.next_server_id + 1

    with Cluster.lock:
        cluster_config = list(Cluster.config.items())

    # Connect the new server to existing ones
    for existing_server_id, data in cluster_config:
        existing_server = data["instance"]
        existing_server.neighbors.append(new_server)
        new_server.neighbors.append(existing_server)
        existing_server.total_nodes += 1

    with Cluster.lock:
        Cluster.config[Cluster.next_server_id] = {"instance": new_server}
    Thread(target=manage_server_lifecycle, args=(Cluster.next_server_id,), daemon=True).start()
    Cluster.next_server_id += 1

    # Pick a random server to send the request to
    with Cluster.lock:
        server_id = random.choice(list(Cluster.config.keys()))
    while Cluster.config[server_id]["instance"].server_state == ServerState.DEAD or isinstance(Cluster.config[server_id]["instance"].role, Joining):
        with Cluster.lock:
            server_id = random.choice(list(Cluster.config.keys()))

    # Send a request to join the cluster
    message = Message(source=new_server.id, destination=server_id, term=Cluster.term_counter, payload=None, message_type=MessageType.RequestToJoin, join_upon_confirmation=True)
    new_server.send_message(message, target_id=server_id)

    Cluster.all_nodes.add(new_server.id)

    print(f"Server {new_server.id} sent a request to join the cluster to server {server_id}.")


def mark_server_as_dead(server_id):
    """
    Marks a server as dead in the cluster.
    """
    Cluster.config[server_id]["instance"].server_state = ServerState.DEAD


def resume_server(server_id):
    """
    Resumes a previously dead server.
    """
    Cluster.config[server_id]["instance"].server_state = ServerState.RESUME
    Thread(target=manage_server_lifecycle, args=(server_id,), daemon=True).start()


def forward_client_request(source_id, payload):
    """
    Forwards a client request to the current leader.
    """
    server = Cluster.config[source_id]["instance"]
    server.handle_client_command(payload)


def start_election():
    """
    Initiates an election in the cluster.
    """

    with Cluster.lock:
        cluster_values = list(Cluster.config.values())

    for server_data in cluster_values:
        server = server_data["instance"]
        if server.server_state == ServerState.FOLLOWER:
            server.server_state = ServerState.CANDIDATE
    Cluster.term_counter += 1


def display_logs_for_server(server_id):
    """
    Displays the log of a specified server.
    """
    server = Cluster.config[server_id]["instance"]
    print(f"Logs for server {server_id}: {server.log}")


# ----------------------- Simulation Framework -----------------------

class RaftSimulation:
    def __init__(self, num_servers, simulation_duration, leader_fail_frequency=None, leader_recover_frequency=None, add_node_frequency=None, fail_node_frequency=None, quiet=False):
        """
        Initializes the simulation.

        :param num_servers: Number of servers in the cluster.
        :param simulation_duration: Total duration of the simulation in seconds.
        :param leader_fail_frequency: Time in seconds before randomly failing the leader (optional).
        :param leader_recover_frequency: Time in seconds before recovering a failed leader (optional).
        :param quiet: If True, reduces console output to essential messages only.
        """
        self.num_servers = num_servers
        self.simulation_duration = simulation_duration
        self.leader_fail_frequency = leader_fail_frequency
        self.leader_recover_frequency = leader_recover_frequency
        self.add_node_frequency = add_node_frequency
        self.fail_node_frequency = fail_node_frequency
        self.start_time = None
        self.total_election_time = 0  # Total time spent in leader elections
        self.election_times = []
        self.commit_times = []
        self.last_known_leader = None
        self.lock = Lock()  # Lock for thread safety

    def initialize_cluster(self):
        """Starts the simulation cluster by spawning servers and initiating an election."""
        print(f"Initializing cluster with {self.num_servers} servers...")
        Thread(target=monitor_all_servers, daemon=True).start()

        # Spawn servers
        for i in range(self.num_servers):
            add_server_to_cluster()
            time.sleep(0.25)
            
            if i == 2:
                print("3 servers have been added. Initiating the first election...")
                self.start_time = time.time()
                start_election()
                self.wait_for_election_completion()


    def wait_for_election_completion(self):
        """
        Waits for an election to complete by monitoring the state of the cluster.

        :return: Time taken to complete the election in seconds.
        """
        while True:
            leader_id = self.get_leader_id()
            with self.lock:
                if leader_id is not None and leader_id != self.last_known_leader:
                    election_time = time.time() - self.start_time
                    self.election_times.append(election_time)
                    self.total_election_time += election_time  # Increment total election time
                    print(f"New leader elected in {election_time:.3f} seconds: server {leader_id}")
                    self.last_known_leader = leader_id
                    return election_time
            time.sleep(0.001)  # Avoid tight polling

    def run_simulation(self):
        """Runs the simulation for the specified duration."""
        print(f"Running simulation for {self.simulation_duration} seconds...")
        end_time = time.time() + self.simulation_duration
        buffer_time = end_time + 5  # Add 5 seconds to allow for cleanup

        while time.time() < end_time:
            # Randomly send client commands to simulate activity
            server = randint(0, len(Cluster.config) - 1)
            while Cluster.config[server]["instance"].server_state == ServerState.DEAD or isinstance(Cluster.config[server]["instance"].role, Joining):
                server = randint(0, len(Cluster.config) - 1)
            message_data = randint(1, 100000)
            self.commit_start_time = time.time()
            forward_client_request(server, message_data)

            # Monitor log commit time
            time.sleep(0.1)  # Simulate a short delay between commands
            commit_time = time.time() - self.commit_start_time - 0.1  # Subtract delay time
            self.commit_times.append(commit_time)

        # If in the middle of an election, add the remaining time to the total election time
        if self.get_leader_id() is None:
            election_time = end_time - self.start_time
            self.election_times.append(election_time)
            self.total_election_time += election_time
            print(f"Leader election in progress at end of simulation. Adding {election_time:.3f} seconds to total.")

        # Wait 5 seconds for any remaining messages to be processed
        print("Simulation complete. Waiting for messages to be processed...")

        while time.time() < buffer_time:
            time.sleep(0.001)


    def add_node_periodically(self):
        """Periodically adds a new node to the cluster if specified."""
        if not self.add_node_frequency:
            return

        def add_node():
            while True:
                time.sleep(self.add_node_frequency)
                add_request_to_join_cluster()

        Thread(target=add_node, daemon=True).start()

    def fail_leader_periodically(self):
        """Periodically fails the leader if specified."""
        if not self.leader_fail_frequency:
            return
        
        end_time = time.time() + (self.simulation_duration // 2)

        def fail_leader():
            while time.time() < end_time:
                time.sleep(self.leader_fail_frequency)
                leader_id = self.get_leader_id()
                if leader_id is not None:
                    print(f"Simulating leader failure for server {leader_id}...")
                    self.start_time = time.time()  # Record start time for election
                    mark_server_as_dead(leader_id)
                    time.sleep(0.001) # Blocking call acts as a barrier
                    self.wait_for_election_completion()

        Thread(target=fail_leader, daemon=True).start()

    def fail_node_periodically(self):
        """Periodically fails a random node if specified."""
        if not self.fail_node_frequency:
            return

        end_time = time.time() + (self.simulation_duration // 2)

        def fail_node():
            while time.time() < end_time:
                time.sleep(self.fail_node_frequency)
                server_id = random.choice(list(Cluster.config.keys()))
                if Cluster.config[server_id]["instance"].server_state != ServerState.DEAD:
                    print(f"Simulating failure for server {server_id}...")
                    mark_server_as_dead(server_id)

        Thread(target=fail_node, daemon=True).start()

    def recover_leader_periodically(self):
        """Periodically recovers failed leaders if specified."""
        if not self.leader_recover_frequency:
            return
        
        start_time = time.time() + (self.simulation_duration // 2)
        end_time = time.time() + self.simulation_duration

        def recover_leader():
            while time.time() < start_time:
                time.sleep(0.1)  # Wait for the first leader failure to occur
            while time.time() < end_time:
                failed_servers = [name for name, server in Cluster.config.items() if server["instance"].server_state == ServerState.DEAD]
                if failed_servers:
                    leader_id = random.choice(failed_servers)
                    print(f"Recovering leader with ID {leader_id}...")
                    resume_server(leader_id)
                time.sleep(self.leader_recover_frequency)

        Thread(target=recover_leader, daemon=True).start()

    def get_leader_id(self):
        """Returns the ID of the current leader."""
        for name, server in Cluster.config.items():
            if isinstance(server["instance"].role, Leader):
                return name
        return None

    def benchmark(self):
        """Prints benchmarking statistics."""
        avg_election_time = sum(self.election_times) / len(self.election_times) if self.election_times else 0
        avg_commit_time = sum(self.commit_times) / len(self.commit_times) if self.commit_times else 0
        election_overhead = (self.total_election_time / self.simulation_duration) * 100  # Overhead percentage
        print("\n--- Benchmarking Results ---")
        print(f"Average leader election time: {avg_election_time:.3f} seconds (over {len(self.election_times)} elections)")
        print(f"Leader election overhead: {election_overhead:.2f}% of total simulation time")

    def run(self):
        """Runs the entire simulation."""
        self.initialize_cluster()

        # Start periodic leader failure/recovery if specified (and now addition)
        self.fail_leader_periodically()
        self.recover_leader_periodically()
        # self.add_node_periodically()
        self.fail_node_periodically()

        # Run the simulation
        self.run_simulation()

        # Display benchmarks
        self.benchmark()

        # Display each servers logs
        for name, server in Cluster.config.items():
            # compute hash of logs and print
            if server["instance"].server_state != ServerState.DEAD and server["instance"].server_state != ServerState.JOINING:
                logs = server["instance"].log
                print(f"Server {name} logs hash: {hash(str(logs))}, log length: {len(logs)}")

        log0 = set([str(log) for log in Cluster.config[0]["instance"].log])
        setDiff = Cluster.committed_messages.difference(log0)
        print(f"{len(log0) - len(setDiff)} / {len(log0)} ({100 * (len(log0) - len(setDiff)) / len(log0)}%) of commited logs received\n")
        print(f"Committed logs lost: {len(setDiff)}\n")


# ----------------------- Main Program -----------------------

if __name__ == "__main__":
    # Parameters for the simulation
    num_servers = 20
    simulation_duration = 45  # Run simulation for 30 seconds
    leader_fail_frequency = 5  # Fail leader every 5 seconds
    leader_recover_frequency = 6  # Recover leader every 6 seconds
    add_node_frequency = 6  # Add a new node every 6 seconds
    fail_node_frequency = 2  # Fail a random node every 4 seconds
    quiet = True  # Enable quiet mode

    # Initialize and run the simulation
    simulation = RaftSimulation(
        num_servers=num_servers,
        simulation_duration=simulation_duration,
        leader_fail_frequency=leader_fail_frequency,
        leader_recover_frequency=leader_recover_frequency,
        add_node_frequency=add_node_frequency,
        fail_node_frequency=fail_node_frequency,
        quiet=quiet
    )
    simulation.run()
