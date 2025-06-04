package com.raft.node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.gson.Gson;
import com.raft.rpc.HeartbeatMessage;
import com.raft.rpc.RpcMessage;
import com.raft.rpc.RpcResponse;
import com.raft.rpc.VoteMessage;

public class RaftNode {
    private final InetAddress address;
    private final int port;
    private NodeType nodeType;
    private final List<ServerInfo> clusterMembers;
    private final Map<String, String> dataStore;
    private final Map<SocketChannel, ByteBuffer> clientBuffers;
    private final ExecutorService executorService;
    private final Map<ServerInfo, NodeConnection> nodeConnections;
    private final ScheduledExecutorService heartbeatExecutor;
    private ServerSocketChannel serverSocket;
    private Selector selector;
    private volatile boolean running;
    private long currentTerm;
    private final Gson gson;
    private final float heartbeatTimeout;
    private long lastHeartbeatReceived;
    private String votedFor;
    private int receivedVotes;
    private final Object voteLock = new Object();
    private long lastLogIndex;
    private long lastLogTerm;
    private boolean inElection;
    private final ServerInfo self;
    private final List<LogEntry> log = new ArrayList<>();



    // Constants
    private static final long HEARTBEAT_INTERVAL = 5000; // 2 seconds
    private static final Random random = new Random();

    private int commitIndex = -1;
    private int lastApplied = -1;

    private final Map<ServerInfo, Integer> matchIndex = new ConcurrentHashMap<>();
    private final Map<ServerInfo, Integer> nextIndex = new ConcurrentHashMap<>();


    public RaftNode(String hostAddress, int port, NodeType nodeType) throws IOException {
        this.address = InetAddress.getByName(hostAddress);
        this.port = port;
        this.nodeType = nodeType;
        this.dataStore = new ConcurrentHashMap<>();
        this.clientBuffers = new ConcurrentHashMap<>();
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        this.clusterMembers = new ArrayList<>();
        this.nodeConnections = new ConcurrentHashMap<>();
        this.currentTerm = 0;
        this.gson = new Gson();
        
        // Initialize random heartbeat timeout between 5000-7000 milliseconds
        this.heartbeatTimeout = 5000 + random.nextFloat() * 2000;
        this.lastHeartbeatReceived = System.currentTimeMillis();
        
        this.votedFor = null;
        this.receivedVotes = 0;
        this.lastLogIndex = 0;
        this.lastLogTerm = 0;
        this.self = new ServerInfo(address.getHostName(), port, nodeType);

        
        initializeClusterMembers();
        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) {
                this.nodeConnections.put(member, new NodeConnection(member));
            }
            System.out.println("Cluster member: " + member.getHost() + ":" + member.getPort());
        }

        if (nodeType == NodeType.LEADER) {
            System.out.println("Node ini adalah leader");
            for (ServerInfo follower : clusterMembers) {
                if (!follower.equals(self)) {
                    nextIndex.put(follower, log.size());     // start after last log index
                    matchIndex.put(follower, -1);            // belum ada yang cocok
                }
            }
            startHeartbeat();
        } else {
            System.out.printf("Node ini adalah follower dengan timeout %.2f ms%n", heartbeatTimeout);
            startTimeoutChecker();
        }
    }

    private void initializeClusterMembers() {
        clusterMembers.add(new ServerInfo("raft-node1", 8001, NodeType.FOLLOWER));
        clusterMembers.add(new ServerInfo("raft-node2", 8002, NodeType.FOLLOWER));
        clusterMembers.add(new ServerInfo("raft-node3", 8003, NodeType.FOLLOWER));
        clusterMembers.add(new ServerInfo("raft-node4", 8004, NodeType.FOLLOWER));

        // clusterMembers.add(new ServerInfo("localhost", 8001, NodeType.FOLLOWER));
        // clusterMembers.add(new ServerInfo("localhost", 8002, NodeType.FOLLOWER));
        // clusterMembers.add(new ServerInfo("localhost", 8003, NodeType.FOLLOWER));
        // clusterMembers.add(new ServerInfo("localhost", 8004, NodeType.FOLLOWER));
    }

    private void startTimeoutChecker() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            if (nodeType == NodeType.LEADER) {
                return;  // Leader tidak perlu mengecek timeout
            }
            
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatReceived > heartbeatTimeout && !inElection) {
                System.out.println("No heartbeat received for " + (now - lastHeartbeatReceived) + " ms. Starting election!");
                startElection();
            }
        }, 1000, 1000, MILLISECONDS);
    }

    private void startHeartbeat() {
        if (nodeType != NodeType.LEADER) {
            return;
        }

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
                NodeConnection connection = entry.getValue();
                
                // Try to connect if not connected
                if (!connection.isConnected()) {
                    try{
                        connection.connect();

                    } catch (IOException e) {
                        System.out.println("Error connecting to " + entry.getKey() + ": " + e.getMessage());
                    }
                }
                
                // Send heartbeat if connected
                if (connection.isConnected()) {
                    String leaderId = address.getHostAddress() + ":" + port;
                    if (!connection.sendHeartbeat(leaderId, currentTerm)) {
                        System.err.println("Failed to send heartbeat to " + entry.getKey());
                    }
                }
            }
        }, 0, HEARTBEAT_INTERVAL, MILLISECONDS);
    }

    private void handleHeartbeat(String message) {
        try {
            HeartbeatMessage heartbeat = gson.fromJson(message, HeartbeatMessage.class);
            
            // Only print on even timestamps
            if (lastHeartbeatReceived % 2 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("--------------------------------");
                System.out.println("Received heartbeat from leader: " + heartbeat.getLeaderId() + 
                                 " (term: " + heartbeat.getTerm() + " heartbeat got on " + (now-this.lastHeartbeatReceived) + " , timestamp : "+now+")");
                System.out.println("--------------------------------");
            }
            lastHeartbeatReceived = System.currentTimeMillis();
            inElection = false;  // Reset election state when heartbeat received
            
            // Update term if necessary
            if (heartbeat.getTerm() > currentTerm) {
                currentTerm = heartbeat.getTerm();
            }
        } catch (Exception e) {
            System.err.println("Error processing heartbeat: " + e.getMessage());
        }
    }

    private void startElection() {
        synchronized (voteLock) {
            if (inElection) {
                return;  // Prevent multiple elections at the same time
            }
            inElection = true;
            nodeType = NodeType.CANDIDATE;
            currentTerm++;
            votedFor = address.getHostAddress() + ":" + port;
            receivedVotes = 1; // Vote for self
            System.out.println("Starting election for term " + currentTerm);
        }

        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) { // Don't add connection to self
                nodeConnections.putIfAbsent(member, new NodeConnection(member));
            }
        }

        // Send RequestVote RPCs to all other servers

        VoteMessage.VoteRequest voteRequest = new VoteMessage.VoteRequest(
            votedFor,
            currentTerm,
            lastLogIndex,
            lastLogTerm
        );

        for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
            NodeConnection connection = entry.getValue();
            if (!connection.isConnected()) {
                try {

                    System.out.println("MASUKKK IS CONNECTED !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    // boolean Connected = connection.connect();
                    // if (!Connected) {
                    //     SocketChannel peerChannel = connection.getChannel();
                    //     peerChannel.register(this.selector, SelectionKey.OP_CONNECT, voteRequest); // Attach voteRequest
                    //     this.selector.wakeup();
                    //     continue;
                    // }
                    connection.connect();
                    
                } catch (IOException e) {
                    System.err.println("Error starting connection to " + entry.getKey() + ": " + e.getMessage());
                    continue;
                }
            }

            if (connection.isConnected()) {
                executorService.submit(() -> sendVoteRequest(connection, voteRequest));
            }
        }

        // Set timer untuk election
        heartbeatExecutor.schedule(() -> {
            synchronized (voteLock) {
                if (nodeType == NodeType.CANDIDATE) {
                    System.out.println("Election timeout - reverting to follower");
                    nodeType = NodeType.FOLLOWER;
                    inElection = false;  // Reset election state
                    votedFor = null;  // Reset vote
                }
            }
        }, 10000, MILLISECONDS);
    }

    private void sendVoteRequest(NodeConnection connection, VoteMessage.VoteRequest request) {
        try {
            String jsonRequest = gson.toJson(request);
            connection.send(jsonRequest);
            
            // Register channel untuk membaca response
            SocketChannel channel = connection.getChannel();
            channel.configureBlocking(false);
            SelectionKey key = channel.keyFor(selector);
            
            // Fix: Ensure proper registration and buffer allocation
            clientBuffers.putIfAbsent(channel, ByteBuffer.allocate(1024));
            if (key == null || !key.isValid()) {
                key = channel.register(selector, SelectionKey.OP_READ);
                System.out.println("Registered new channel for reading vote responses");
            } else {
                key.interestOps(SelectionKey.OP_READ);
                System.out.println("Updated existing channel for reading vote responses");
            }
            selector.wakeup(); // Wake up selector to process new registrations
        } catch (Exception e) {
            System.err.println("Error sending vote request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleVoteRequest(SocketChannel channel, VoteMessage.VoteRequest request) {
        System.out.println("Handling vote request");
        boolean voteGranted = false;
        synchronized (voteLock) {
            if (request.getTerm() < currentTerm) {
                voteGranted = false;
            } 
            else if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                nodeType = NodeType.FOLLOWER;
                votedFor = null;
                if (request.getLastLogIndex() >= lastLogIndex && request.getLastLogTerm() >= lastLogTerm) {
                    votedFor = request.getCandidateId();
                    voteGranted = true;
                } else {
                    voteGranted = false;
                }
            } 
            else { // request.getTerm() == currentTerm
                if ((votedFor == null || votedFor.equals(request.getCandidateId())) &&
                   (request.getLastLogIndex() >= lastLogIndex && request.getLastLogTerm() >= lastLogTerm)) {
                    votedFor = request.getCandidateId();
                    voteGranted = true;
                } else {
                    voteGranted = false;
                }
            }
        }
        System.out.println("voted for : " + votedFor);

        VoteMessage.VoteResponse response = new VoteMessage.VoteResponse(
            currentTerm,
            voteGranted,
            address.getHostAddress() + ":" + port
        );

        long now = System.currentTimeMillis();
        System.out.println("--------------------------------");
        System.out.println("created vote response for " + request.getCandidateId() + " response : " + voteGranted + " at " + now/1000 + " s with response type : " + response.getType());

        try {
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
            System.out.println("--------------------------------");
            System.out.println("Write Buffer to " + channel.getRemoteAddress() + " done");

        } catch (IOException e) {
            System.err.println("Error sending vote response: " + e.getMessage());
        }
    }

    private void handleVoteResponse(VoteMessage.VoteResponse response) {
        System.out.println("=== Vote Response Processing ===");
        System.out.println("Current node type: " + nodeType);
        System.out.println("Response from: " + response.getVoterId());
        System.out.println("Vote granted: " + response.isVoteGranted());
        System.out.println("Current votes: " + receivedVotes);
        System.out.println("handling vote response from " + response.getVoterId() + " with term " + response.getTerm());

        if (nodeType != NodeType.CANDIDATE) {
            System.out.println("Ignoring vote - no longer a candidate");
            return;
        }

        if (response.getTerm() > currentTerm) {
            currentTerm = response.getTerm();
            nodeType = NodeType.FOLLOWER;
            votedFor = null;
            return;
        }

        if (response.isVoteGranted()) {
            synchronized (voteLock) {
                receivedVotes++;
                System.out.println("Received vote from " + response.getVoterId() + 
                                 ". Total votes: " + receivedVotes);

                // Hitung node yang aktif (tidak termasuk node yang down)
                int activeNodes = 1; // Diri sendiri
                for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
                    if (entry.getValue().isConnected()) {
                        activeNodes++;
                    }
                }
                
                // Check if we have majority dari node yang aktif
                if (receivedVotes > activeNodes / 2) {
                    becomeLeader();
                }
            }
        }
    }

    private void becomeLeader() {
        if (nodeType == NodeType.CANDIDATE) {
            System.out.println("Becoming leader for term " + currentTerm);
            nodeType = NodeType.LEADER;
            inElection = false;  // Reset election state

            // Update the corresponding ServerInfo in clusterMembers
            for (ServerInfo member : clusterMembers) {
                if (member.getPort() == this.port) {
                    member.setType(NodeType.LEADER);
                    System.out.println("Updated cluster member status to LEADER: " + member);
                }
            }
            
            votedFor = null;
            startHeartbeat();
        }
    }

    public void startServer() throws IOException {
        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(address, port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        
        System.out.printf("RaftNode (%s) berjalan di %s:%d%n", nodeType, address.getHostAddress(), port);

        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

                while (keys.hasNext()) {
                    System.out.println("!!!!!!!!!!!! MASUK WHILEEEEE !!!!!!!!!!!!");
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        System.out.println("!!!!!!!!! ACCEPTTTTT !!!!!!!!!!!!!!");
                        accept(key);
                    } else if (key.isReadable()) {
                        System.out.println("!!!!!!!!! READDDDDD !!!!!!!!!!!!!!");
                        read(key);
                    }else if (key.isConnectable()) {
                        System.out.println("!!!!!!!!!!!!! CONNECT !!!!!!!!!!!!");
                        handleConnect(key);
                    }
                }
            } catch (IOException e) {
                System.err.println("Error dalam event loop: " + e.getMessage());
            }
        }
    }
    
    private void handleConnect(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        NodeConnection connection = findNodeConnectionForChannel(channel);
        try {
            if (channel.finishConnect()) {
                System.out.println("Successfully connected to " + connection.getServerInfo());
                if (connection != null) {
                    connection.setConnected(true);
                    clientBuffers.putIfAbsent(channel, ByteBuffer.allocate(1024));
                }
                key.interestOps(0);
                
                if (key.attachment() instanceof VoteMessage.VoteRequest) {
                    executorService.submit(() -> sendVoteRequest(connection, (VoteMessage.VoteRequest) key.attachment()));
                } else {
                    channel.register(this.selector, SelectionKey.OP_READ);
                    clientBuffers.putIfAbsent(channel, ByteBuffer.allocate(1024));
                    this.selector.wakeup();
                }
            } else {
                // Koneksi gagal
                // System.err.println("Failed to finish connect to " + (connection != null ? connection.getServerInfo() : channel.getRemoteAddress()));
                key.cancel();
                if (connection != null) connection.disconnect();
            }
        } catch (IOException e) {
            // System.err.println("Exception during finishConnect to " + (connection != null ? connection.getServerInfo() : getRemoteAddressSafe(channel)) + ": " + e.getMessage());
            key.cancel();
            if (connection != null) connection.disconnect();
        }
    }

    private NodeConnection findNodeConnectionForChannel(SocketChannel channel) {
        for (NodeConnection nc : nodeConnections.values()) {
            if (nc.getChannel() == channel) { // Perbandingan referensi, pastikan ini benar
                return nc;
            }
        }
        return null;
    }

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        clientChannel.configureBlocking(false);
        clientChannel.register(selector, SelectionKey.OP_READ);
        clientBuffers.put(clientChannel, ByteBuffer.allocate(1024));
        System.out.println("Client/Node terhubung: " + clientChannel.getRemoteAddress());
    }

    private void read(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = clientBuffers.get(channel);
        System.out.println(" !!!!!! MASUK READDDDDDD !!!!");

        if (buffer == null) {
            System.err.println("No buffer found for channel: " + channel);
            return;
        }
        
        try {
            int bytesRead = channel.read(buffer);
            System.out.println("Read " + bytesRead + " bytes from " + channel.getRemoteAddress());
            if (bytesRead == -1) {
                System.out.println("--------------------------------");
                System.out.println("bytes read : " + bytesRead + " connection closed from " + channel.getRemoteAddress());
                System.out.println("--------------------------------");
                closeConnection(channel);
                return;
            }

            if (bytesRead > 0) {
                buffer.flip();
                byte[] data = new byte[buffer.limit()];
                buffer.get(data);
                buffer.clear();

                String message = new String(data).trim();
                System.out.println("--------------------------------");
                System.out.println("message recieved from " + channel.getRemoteAddress() + " : " + message);
                System.out.println("--------------------------------");
                
                System.out.println("MASUKKKKKKKKK SINIIIIIIII !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                // Parse message to determine type
                try {
                    if (message.contains("\"type\":\"HEARTBEAT\"")) {
                        handleHeartbeat(message);
                    } else if (message.contains("\"type\":\"VOTE_REQUEST\"")) {
                        System.out.println("MASUK KE VOTE REQUESTTTTTTTTTTTTTTTTTTT !!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        inElection = true;
                        System.out.println("Received vote request from " + channel.getRemoteAddress());
                        VoteMessage.VoteRequest voteRequest = gson.fromJson(message, VoteMessage.VoteRequest.class);
                        handleVoteRequest(channel, voteRequest);
                    } else if (message.contains("\"type\":\"VOTE_RESPONSE\"")) {
                        System.out.println("MASUK KE VOTE RESPONSEEEEEEEEEEEEEEEEEEEEEE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        System.out.println("Received vote response from " + channel.getRemoteAddress());
                        VoteMessage.VoteResponse voteResponse = gson.fromJson(message, VoteMessage.VoteResponse.class);
                        handleVoteResponse(voteResponse);
                    } else if( message.contains("\"type\":\"APPEND_ENTRIES\"")) {
                        System.out.println("MASUK KE APPEND ENTRIESEEEEEEEEEEEEEEEEEEEE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        Map<String, Object> params = gson.fromJson(message, Map.class);
                        RpcResponse ack = handleAppendEntries(params);
                        sendResponse(channel, ack);
                    } else {
                        System.out.println("MASUK KE PROCESS CLIENT MESSAGEEEEEEEEEEEEEEEEEEEEE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        processClientMessage(channel, message);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading from channel: " + e.getMessage());
            closeConnection(channel);
        }
    }

    private void processClientMessage(SocketChannel clientChannel, String message) {
        executorService.submit(() -> {
            try {
                RpcMessage rpcMessage = RpcMessage.fromJson(message);
                RpcResponse response;
                
                if (nodeType != NodeType.LEADER) {
                    // If not leader, return error with leader information
                    ServerInfo leader = clusterMembers.stream()
                        .filter(s -> s.getType() == NodeType.LEADER)
                        .findFirst()
                        .orElse(null);

                    if (leader != null) {
                        response = new RpcResponse(rpcMessage.getId(), 
                            new RpcResponse.RpcError(-32000, 
                                "Not leader. Please connect to leader.", 
                                Map.of("leader_host", leader.getHost(), 
                                      "leader_port", leader.getPort())));
                    } else {
                        response = new RpcResponse(rpcMessage.getId(),
                            new RpcResponse.RpcError(-32001, "Leader not found", null));
                    }
                } else if (rpcMessage.getMethod().equals("appendEntries")) {
                    Map<String, Object> params = (Map<String, Object>) rpcMessage.getParams();
                    RpcResponse ack = handleAppendEntries(params);
                    sendResponse(clientChannel, ack);  // ← kirim ACK ke Leader
                    return; 
                } else {
                    // Process the command if this is the leader
                    synchronized (log) {
                        System.out.println("\n=== Starting Log Replication ===");
                        System.out.println("Command: " + rpcMessage.getMethod());
                        System.out.println("Current term: " + currentTerm);
                        
                        LogEntry entry = new LogEntry(currentTerm, rpcMessage);
                        int entryIndex = log.size();
                        log.add(entry);
                        System.out.println("Entry added to log at index: " + entryIndex);
                        
                        // Start replication to followers
                        System.out.println("Starting replication to followers...");
                        replicateLogToFollowers(entry, entryIndex);
                        
                        // Wait for the entry to be committed
                        System.out.println("Waiting for entry to be committed...");
                        while (commitIndex < entryIndex) {
                            try {
                                Thread.sleep(100); // Wait a bit before checking again
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        System.out.println("Entry committed at index: " + entryIndex);
                        
                        // Process the command and get response
                        System.out.println("Processing command...");
                        response = processCommand(rpcMessage);
                        System.out.println("Command processed. Response: " + response.getResult());
                        System.out.println("=== Log Replication Complete ===\n");
                    }  
                }

                // Send response
                sendResponse(clientChannel, response);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        });
    }

    private void commitEntry(int index) {
        if (index > commitIndex) {
            LogEntry entry = log.get(index);
            RpcMessage cmd = RpcMessage.fromJson(entry.getCommand());  // Convert String to RpcMessage

            // Apply command ke state machine
            processCommand(cmd); 

            commitIndex = index;
        }
    }

    private void replicateLogToFollowers(LogEntry entry, int entryIndex) {
        System.out.println("\n--- Replicating to Followers ---");
        for (ServerInfo follower : clusterMembers) {
            if (!follower.equals(self)) {
                System.out.println("Replicating to follower: " + follower);
                replicateLogEntry(follower, entryIndex);
            }
        }
    }

    private void replicateLogEntry(ServerInfo follower, int entryIndex) {
        if (entryIndex >= log.size()) {
            return;
        }
        LogEntry entry = log.get(entryIndex);
        Map<String, Object> payload = new ConcurrentHashMap<>();
        payload.put("term", currentTerm);
        payload.put("leaderId", self.toString());
        payload.put("prevLogIndex", entryIndex - 1);
        payload.put("prevLogTerm", entryIndex > 0 ? log.get(entryIndex - 1).getTerm() : 0L);
        payload.put("entries", entry);
        payload.put("leaderCommit", commitIndex);

        System.out.println("Sending AppendEntries to " + follower + " for index " + entryIndex);
        sendRpcAsync(follower, "appendEntries", payload, response -> {
            if (response.getError() == null) {
                System.out.println("Follower " + follower + " acknowledged entry at index " + entryIndex);
                matchIndex.put(follower, entryIndex);
                nextIndex.put(follower, entryIndex + 1);
                tryCommitEntries();
            } else {
                System.out.println("Follower " + follower + " rejected entry at index " + entryIndex + 
                                 ". Error: " + response.getError());
                nextIndex.put(follower, Math.max(0, nextIndex.get(follower) - 1));
                replicateLogEntry(follower, nextIndex.get(follower));
            }
        });
    }

    private void sendRpcAsync(ServerInfo follower, String method, Map<String, Object> payload, Consumer<RpcResponse> callback) {
        executorService.submit(() -> {
            try {
                NodeConnection conn = nodeConnections.get(follower);
                if (conn == null || !conn.isConnected()) return;

                RpcMessage message = new RpcMessage(UUID.randomUUID().toString(), method, payload);
                conn.send(message.toJson());

                // Simulate reading response
                SocketChannel channel = conn.getChannel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);
                int read = channel.read(buffer);
                if (read > 0) {
                    buffer.flip();
                    String responseStr = new String(buffer.array(), 0, buffer.limit()).trim();
                    RpcResponse response = RpcResponse.fromJson(responseStr);
                    callback.accept(response);
                } else {
                    callback.accept(RpcResponse.error(-99, "no response"));
                }
            } catch (Exception e) {
                callback.accept(RpcResponse.error(-99, "send failed: " + e.getMessage()));
            }
        });
    }

    private void tryCommitEntries() {
        for (int i = log.size() - 1; i > commitIndex; i--) {
            int finalI = i;
            long count = matchIndex.values().stream()
                .filter(index -> index >= finalI)
                .count() + 1; // +1 untuk leader sendiri

            if (count >= (clusterMembers.size() / 2) + 1 && log.get(i).getTerm() == currentTerm) {
                System.out.println("\n--- Committing Entries ---");
                System.out.println("Committing entries up to index: " + i);
                System.out.println("Number of followers that have replicated: " + (count - 1));
                commitIndex = i;
                applyCommittedEntries();
                break;
            }
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            System.out.println("Applying committed entry at index: " + lastApplied);
            RpcMessage cmd = RpcMessage.fromJson(log.get(lastApplied).getCommand());
            processCommand(cmd);
        }
    }


    private RpcResponse handleAppendEntries(Map<String, Object> params) {
        System.out.println("1");
        long term = ((Number) params.get("term")).longValue();
        int prevLogIndex = ((Number) params.get("prevLogIndex")).intValue();
        long prevLogTerm = ((Number) params.get("prevLogTerm")).longValue();
        @SuppressWarnings("unchecked")
        List<LogEntry> entries = (List<LogEntry>) params.get("entries");
        int leaderCommit = ((Number) params.get("leaderCommit")).intValue();
        System.out.println("2");

        if (term < currentTerm) {
            
            return RpcResponse.error(-1, "Term too old");
        }

        if (prevLogIndex >= 0) {
            if (log.size() <= prevLogIndex || log.get(prevLogIndex).getTerm() != prevLogTerm) {
                return RpcResponse.error(-2, "Log inconsistency");
            }
        }

        // Replace or append log entries
        for (int i = 0; i < entries.size(); i++) {
            int index = prevLogIndex + 1 + i;
            if (log.size() > index) {
                log.set(index, entries.get(i));
            } else {
                log.add(entries.get(i));
            }
        }

        if (leaderCommit > commitIndex) {
            commitIndex = Math.min(leaderCommit, log.size() - 1);
            System.out.println("Commit index updated to " + commitIndex);
            applyCommittedEntries();
        }

        return RpcResponse.success("ACK");
    }


    private RpcResponse processCommand(RpcMessage message) {
        String method = message.getMethod();
        Map<String, Object> params = (Map<String, Object>) message.getParams();

        switch (method) {
            case "ping":
                return new RpcResponse(message.getId(), "pong");
                
            case "get":
                String key = (String) params.get("key");
                String value = dataStore.get(key);
                return new RpcResponse(message.getId(), value != null ? value : null);
                
            case "set":
                key = (String) params.get("key");
                value = (String) params.get("value");
                dataStore.put(key, value);
                return new RpcResponse(message.getId(), true);
            
            case "append":
                key = (String) params.get("key");
                String appendValue = (String) params.get("value");

                boolean isNew = !dataStore.containsKey(key);
                dataStore.put(key, dataStore.getOrDefault(key, "") + appendValue);

                if (isNew) {
                    return new RpcResponse(message.getId(), "OK");
                } else {
                    return new RpcResponse(message.getId(), dataStore.get(key));
                }

            case "del":
                key = (String) params.get("key");
                String removedValue = dataStore.remove(key);
                return new RpcResponse(message.getId(), removedValue != null ? removedValue : "");

            case "strln":
                key = (String) params.get("key");
                String strValue = dataStore.get(key);
                if (strValue != null) {
                    return new RpcResponse(message.getId(), strValue.length());
                } else {
                    return new RpcResponse(message.getId(), 
                        new RpcResponse.RpcError(-32005, "Key not found", null));
                }
                 
            default:
                return new RpcResponse(message.getId(), 
                    new RpcResponse.RpcError(-32601, "Method not found", null));
        }
    }

    private void sendResponse(SocketChannel channel, RpcResponse response) {
        try {
            String jsonResponse = response.toJson();
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
        } catch (IOException e) {
            System.err.println("Error sending response: " + e.getMessage());
            closeConnection(channel);
        }
    }

    private void closeConnection(SocketChannel channel) {
        try {
            clientBuffers.remove(channel);
            channel.close();
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    public void stopServer() {
        running = false;
        executorService.shutdown();
        heartbeatExecutor.shutdown();
        
        // Close all node connections
        for (NodeConnection connection : nodeConnections.values()) {
            connection.disconnect();
        }
        
        try {
            if (selector != null && selector.isOpen()) {
                selector.close();
            }
            if (serverSocket != null && serverSocket.isOpen()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing server: " + e.getMessage());
        }
    }
}
