# Rafted - Distributed Key-Value Store with Raft Consensus

A simple distributed key-value storage system implementing the Raft consensus protocol in Java.

## Features

- Raft consensus protocol implementation
- In-memory distributed key-value store
- Dynamic cluster membership changes
- CLI-based interface
- TCP-based communication

## Building

The project uses Maven for dependency management and building. To build the project:

```bash
mvn clean package
```

This will create a runnable JAR file in the `target` directory.

## Running

To start a node:

```bash
java -jar target/rafted-1.0-SNAPSHOT-jar-with-dependencies.jar <node_id> [<port>]
```

If port is not specified, it defaults to 5000 + node_id.

## Usage

### Starting a Cluster

1. Start the first node:
```bash
java -jar target/rafted-1.0-SNAPSHOT-jar-with-dependencies.jar 1
```

2. Start additional nodes:
```bash
java -jar target/rafted-1.0-SNAPSHOT-jar-with-dependencies.jar 2
java -jar target/rafted-1.0-SNAPSHOT-jar-with-dependencies.jar 3
```

### CLI Commands

- `connect <address> <port>` - Connect to a node
- `add <node_id> <port>` - Add a new node to the cluster
- `remove <node_id>` - Remove a node from the cluster
- `get <key>` - Get value for key
- `set <key> <value>` - Set value for key
- `del <key>` - Delete key
- `strln <key>` - Get length of value
- `append <key> <value>` - Append value to key
- `ping` - Check connection
- `help` - Show help
- `exit` - Exit the program

## Architecture

### Components

1. **RaftNode**: Implements the core Raft protocol
   - Leader election
   - Log replication
   - Heartbeat mechanism
   - Membership changes

2. **NetworkManager**: Handles TCP-based communication
   - Message serialization/deserialization
   - Client connections
   - Inter-node communication

3. **Main**: CLI interface and node management
   - Command parsing
   - Node lifecycle management
   - User interaction

### Communication

- Uses TCP for reliable communication
- JSON-based message format
- Non-blocking I/O for better performance

### Consistency

- Strong consistency (linearizable)
- All writes go through the leader
- Majority quorum for commits
- Follower reads are redirected to leader

## Implementation Details

### Raft Protocol

- Random election timeouts (100-150ms)
- Heartbeat interval: 50ms
- Majority-based consensus
- Log replication with append entries

### Key-Value Store

- In-memory storage
- Basic string operations
- Atomic operations
- Consistent state across nodes

## Limitations

- In-memory storage (no persistence)
- Single-threaded command execution
- No security features
- Basic error handling

## Future Improvements

- Persistent storage
- Security features
- Better error handling
- Performance optimizations
- Monitoring and metrics 