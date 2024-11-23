import time
from threading import Thread, Lock
from random import randint
from variables import *
from server import Server
from follower import Follower
from leader import Leader
from candidate import Candidate
from message import Message
import random
import logging

# Global variables
term = 0
config = {}
available_id = 0

# ----------------------- Server Management -----------------------

def process_server_messages(server):
    """Processes all pending messages for a specific server."""
    while True:
        message = server.get_message()
        if message is None:
            break
        server.on_message(message)

def check_server_messages():
    """Checks messages for all servers."""
    while True:
        time.sleep(0.0001)
        for name, server_data in config.items():
            server = server_data["object"]
            if server._serverState != deadState:
                process_server_messages(server)

def handle_leader(server):
    """Handles leader-specific tasks."""
    if time.time() >= server._state._timeoutTime:
        server._state._send_heart_beat()

def handle_candidate(server):
    """Handles candidate-specific tasks."""
    if time.time() >= server._state._timeoutTime:
        server._state = Follower()
        server._state.set_server(server)

def handle_follower(server):
    """Handles follower-specific tasks."""
    if term >= 1 and time.time() >= server._state._timeoutTime:
        # print(f"{server._name} finds that the leader is dead")
        server._serverState = candidateState

def transition_to_candidate(server):
    """Transitions a server to the candidate state."""
    timeout = randint(10000, 500000) / 1_000_000
    time.sleep(timeout)
    if server._serverState == candidateState:
        server._state = Candidate()
        server._state.set_server(server)

def handle_server_state(name):
    """Runs the server's state machine."""
    server = config[name]["object"]
    initialize_server(server, name)

    while True:
        # Handle specific state tasks
        if isinstance(server._state, Leader):
            handle_leader(server)
        elif isinstance(server._state, Candidate):
            handle_candidate(server)
        elif isinstance(server._state, Follower):
            handle_follower(server)

        # Handle state transitions
        if server._serverState == deadState:
            terminate_server(server, name)
            return
        if server._serverState == candidateState and not isinstance(server._state, Candidate):
            transition_to_candidate(server)

        time.sleep(0.0001)

# ----------------------- Helper Functions -----------------------

def initialize_server(server, name):
    """Initializes or resumes a server."""
    if server._serverState == followerState:
        print(f"Started server with name {name}")
    elif server._serverState == resumeState:
        print(f"Resumed server with name {name}")
        # print(server._commitIndex)
        # print(server._log)
        server._state = Follower()
        server._state.set_server(server)
        server._state.on_resume()
        server._serverState = followerState

def terminate_server(server, name):
    """Terminates a server."""
    print(f"Killed server with name {name}")
    server._state = Follower()
    server._state.set_server(server)
    print(server._commitIndex)

def add_new_server():
    """Adds a new server to the configuration."""
    global available_id
    state = Follower()
    server = Server(available_id, state, [], [])
    server._total_nodes = available_id + 1

    # Connect the new server to existing servers
    is_leader_present = any(isinstance(config[i]["object"]._state, Leader) for i in range(available_id))
    for i in range(available_id):
        neighbor = config[i]["object"]
        server._neighbors.append(neighbor)
        neighbor._total_nodes = available_id + 1
        neighbor._neighbors.append(server)

    if is_leader_present:
        server._state.on_resume()

    # Save the server configuration and start its thread
    config[available_id] = {"object": server}
    Thread(target=handle_server_state, args=(available_id,), daemon=True).start()
    available_id += 1

def kill_server(name):
    """Marks a server as dead."""
    config[name]["object"]._serverState = deadState

def resume_server(name):
    """Resumes a previously killed server."""
    config[name]["object"]._serverState = resumeState
    Thread(target=handle_server_state, args=(name,), daemon=True).start()

def send_client_command(sender, message_data):
    """Sends a client command to a server."""
    server = config[sender]["object"]
    server.on_client_command(message_data)

def initiate_election():
    """Initiates the first election among followers."""
    global term
    for i in range(available_id):
        if config[i]["object"]._serverState == followerState:
            config[i]["object"]._serverState = candidateState
    term = 1

def display_server_logs(name):
    """Displays the logs of a specific server."""
    server = config[name]["object"]
    print(f"Server {name} logs: {server._log}")


# ----------------------- Simulation Framework -----------------------

class RaftSimulation:
    def __init__(self, num_servers, simulation_duration, leader_fail_frequency=None, leader_recover_frequency=None, quiet=False):
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
        self.start_time = None
        self.total_election_time = 0  # Total time spent in leader elections
        self.election_times = []
        self.commit_times = []
        self.last_known_leader = None
        self.lock = Lock()  # Lock for thread safety

        # Set up logging
        self.logger = logging.getLogger("RaftSimulation")
        self.logger.setLevel(logging.INFO if quiet else logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def initialize_cluster(self):
        """Starts the simulation cluster by spawning servers and initiating an election."""
        self.logger.info(f"Initializing cluster with {self.num_servers} servers...")
        Thread(target=check_server_messages, daemon=True).start()

        # Spawn servers
        for _ in range(self.num_servers):
            add_new_server()

        # Wait for servers to initialize
        time.sleep(0.25)

        # Start the first election and benchmark it
        self.logger.info("Initiating the first election...")
        self.start_time = time.time()
        initiate_election()
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
                    self.logger.info(f"New leader elected in {election_time:.3f} seconds.")
                    self.last_known_leader = leader_id
                    return election_time
            time.sleep(0.001)  # Avoid tight polling

    def run_simulation(self):
        """Runs the simulation for the specified duration."""
        self.logger.info(f"Running simulation for {self.simulation_duration} seconds...")
        end_time = time.time() + self.simulation_duration

        while time.time() < end_time:
            # Randomly send client commands to simulate activity
            server = randint(0, len(config) - 1)
            message_data = f"Message at {time.time()}"
            self.logger.debug(f"Sending client command to server {server}: {message_data}")
            self.start_time = time.time()
            send_client_command(server, message_data)

            # Monitor log commit time
            time.sleep(0.1)  # Simulate a short delay between commands
            commit_time = time.time() - self.start_time - 0.1  # Subtract delay time
            self.commit_times.append(commit_time)

    def fail_leader_periodically(self):
        """Periodically fails the leader if specified."""
        if not self.leader_fail_frequency:
            return

        def fail_leader():
            while True:
                time.sleep(self.leader_fail_frequency)
                leader_id = self.get_leader_id()
                if leader_id is not None:
                    self.logger.info(f"Simulating leader failure for server {leader_id}...")
                    self.start_time = time.time()  # Record start time for election
                    self.last_known_leader = None  # Reset last known leader
                    kill_server(leader_id)
                    time.sleep(0.001) # Blocking call acts as a barrier
                    self.wait_for_election_completion()

        Thread(target=fail_leader, daemon=True).start()

    def recover_leader_periodically(self):
        """Periodically recovers failed leaders if specified."""
        if not self.leader_recover_frequency:
            return

        def recover_leader():
            while True:
                time.sleep(self.leader_recover_frequency)
                failed_servers = [name for name, server in config.items() if server["object"]._serverState == deadState]
                if failed_servers:
                    leader_id = random.choice(failed_servers)
                    self.logger.info(f"Recovering leader with ID {leader_id}...")
                    resume_server(leader_id)

        Thread(target=recover_leader, daemon=True).start()

    def get_leader_id(self):
        """Returns the ID of the current leader."""
        for name, server in config.items():
            if isinstance(server["object"]._state, Leader):
                return name
        return None

    def benchmark(self):
        """Prints benchmarking statistics."""
        avg_election_time = sum(self.election_times) / len(self.election_times) if self.election_times else 0
        avg_commit_time = sum(self.commit_times) / len(self.commit_times) if self.commit_times else 0
        election_overhead = (self.total_election_time / self.simulation_duration) * 100  # Overhead percentage
        self.logger.info("\n--- Benchmarking Results ---")
        self.logger.info(f"Average leader election time: {avg_election_time:.3f} seconds over {len(self.election_times)} elections")
        self.logger.info(f"Average log commit time: {avg_commit_time:.3f} seconds")
        self.logger.info(f"Leader election overhead: {election_overhead:.2f}% of total runtime")

    def run(self):
        """Runs the entire simulation."""
        self.initialize_cluster()

        # Start periodic leader failure/recovery if specified
        self.fail_leader_periodically()
        self.recover_leader_periodically()

        # Run the simulation
        self.run_simulation()

        # Display benchmarks
        self.benchmark()


# ----------------------- Main Program -----------------------

if __name__ == "__main__":
    # Parameters for the simulation
    num_servers = 10
    simulation_duration = 30  # Run simulation for 30 seconds
    leader_fail_frequency = 5  # Fail leader every 5 seconds
    leader_recover_frequency = 10  # Recover leader every 10 seconds
    quiet = True  # Enable quiet mode

    # Initialize and run the simulation
    simulation = RaftSimulation(
        num_servers=num_servers,
        simulation_duration=simulation_duration,
        leader_fail_frequency=leader_fail_frequency,
        leader_recover_frequency=leader_recover_frequency,
        quiet=quiet
    )
    simulation.run()
