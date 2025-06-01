# Raft Key-Value Store

A distributed key-value store implementation using the Raft consensus protocol. This implementation provides strong consistency guarantees and fault tolerance through the Raft protocol.

## Features

- Raft consensus protocol implementation
- In-memory key-value store
- Leader election
- Log replication
- Heartbeat mechanism
- Membership changes
- Non-blocking I/O using Java NIO
- JSON-RPC over TCP
- CLI interface for client interactions

## Building

The project uses Maven for dependency management and building. To build the project:

```bash
mvn clean package
```

## Running

### Starting the Servers

The system consists of 4 Raft servers and 1 client node. To start the servers:

```bash
# Terminal 1
java -cp target/raft-kvstore-1.0-SNAPSHOT.jar com.raft.server.RaftServerMain server1

# Terminal 2
java -cp target/raft-kvstore-1.0-SNAPSHOT.jar com.raft.server.RaftServerMain server2

# Terminal 3
java -cp target/raft-kvstore-1.0-SNAPSHOT.jar com.raft.server.RaftServerMain server3

# Terminal 4
java -cp target/raft-kvstore-1.0-SNAPSHOT.jar com.raft.server.RaftServerMain server4
```

Each server will automatically connect to the other servers using the following configuration:
- server1: localhost:8001
- server2: localhost:8002
- server3: localhost:8003
- server4: localhost:8004

### Starting the Client

To start the CLI client:

```bash
java -cp target/raft-kvstore-1.0-SNAPSHOT.jar com.raft.cli.RaftCLI
```

The client will automatically connect to all servers and discover the current leader.

## Available Commands

- `get <key>` - Retrieve the value for a key
- `set <key> <value>` - Set a key-value pair
- `del <key>` - Delete a key
- `append <key> <value>` - Append a value to an existing key
- `ping` - Check server availability
- `exit` - Exit the CLI

## Implementation Details

### Raft Protocol

The implementation follows the Raft protocol specification with the following components:

1. **Leader Election**
   - Random election timeouts
   - Term-based voting
   - Majority-based leader selection

2. **Log Replication**
   - AppendEntries RPC
   - Log consistency checks
   - Majority-based commit

3. **Safety**
   - Leader-only writes
   - Log matching property
   - State machine safety

4. **Membership Changes**
   - Joint consensus
   - Configuration changes

### Network Communication

- Uses Java NIO for non-blocking I/O
- JSON-RPC over TCP for message serialization
- Heartbeat mechanism for leader detection
- Automatic leader discovery by clients

### Consistency

- Strong consistency through Raft protocol
- Linearizable reads and writes
- Majority-based commit for durability
- Log-based state machine replication

## Dependencies

- Jackson (JSON processing)
- SLF4J (Logging)
- JUnit (Testing)

## License

This project is licensed under the MIT License - see the LICENSE file for details. 