import time
from threading import Thread
from random import randint
from variables import *
from server import Server
from follower import Follower
from leader import Leader
from candidate import Candidate
from message import Message

# Global variables
term = 0
config = {}
available_id = 0

# ----------------------- Core Functions -----------------------

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

# ----------------------- User Interface -----------------------

def display_menu():
    """Displays the command menu."""
    menu_options = [
        "1. Start a new server",
        "2. Kill a server [Usage: Enter server ID]",
        "3. Resume a server [Usage: Enter server ID]",
        "4. Send a client command [Usage: server ID, message]",
        "5. Initiate the first election",
        "6. Display server logs [Usage: Enter server ID]"
    ]

    print("\n=== Raft Simulation Command Menu ===")
    for option in menu_options:
        print(f"- {option}")
    print("====================================")

def handle_user_input():
    """Processes user input commands."""
    display_menu()
    while True:
        command = input("Enter command: ")
        args = command.split()

        if args[0] == "1":  # Start a new server
            add_new_server()
        elif args[0] == "2":  # Kill a server
            kill_server(int(args[1]))
        elif args[0] == "3":  # Resume a server
            resume_server(int(args[1]))
        elif args[0] == "4":  # Send client command
            send_client_command(int(args[1]), args[2])
        elif args[0] == "5":  # Initiate first election
            initiate_election()
        elif args[0] == "6":    # Display server logs
            display_server_logs(int(args[1]))

# ----------------------- Main Program -----------------------

if __name__ == "__main__":
    # Start the message processing thread
    Thread(target=check_server_messages, daemon=True).start()

    # Process user input
    handle_user_input()
