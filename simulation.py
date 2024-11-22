import time
from threading import Thread, Lock
from random import randint
from variables import *
from server import Server
from follower import Follower
from leader import Leader
from candidate import Candidate
from message import Message
from cli import *
import random
import logging


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
        election_time = self.wait_for_election_completion()
        self.election_times.append(election_time)
        self.total_election_time += election_time
        self.logger.info(f"Leader elected in {election_time:.3f} seconds.")
        self.last_known_leader = self.get_leader_id()

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
            time.sleep(0.01)  # Avoid tight polling

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
            commit_time = time.time() - self.start_time
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
                    time.sleep(0.0001)  # Barrier to prevent reordering
                    # Directly call election completion in the same thread
                    election_time = self.wait_for_election_completion()

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
