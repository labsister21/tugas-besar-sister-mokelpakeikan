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
import java.util.Set;
import java.util.HashSet;  // Add this import
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
    private final Map<String, CommandVote> commandVotes = new ConcurrentHashMap<>();
    private final int VOTE_TIMEOUT = 5000; // 5 seconds timeout for votes

    // Constants
    private static final long HEARTBEAT_INTERVAL = 5000; // 2 seconds
    private static final Random random = new Random();

    private static class CommandVote {
        final RpcMessage command;
        final Set<String> votes = ConcurrentHashMap.newKeySet();
        final Set<String> replicatedNodes = ConcurrentHashMap.newKeySet();
        final long timestamp;
        final long logIndex;
        volatile boolean executed = false;
        volatile boolean committed = false;

        CommandVote(RpcMessage command, long logIndex) {
            this.command = command;
            this.timestamp = System.currentTimeMillis();
            this.logIndex = logIndex;
        }
    }

    private static class AppendEntriesMessage {
        final String type = "APPEND_ENTRIES";
        final String leaderId;
        final long term;
        final RpcMessage command;
        final long prevLogIndex;
        final long prevLogTerm;

        AppendEntriesMessage(String leaderId, long term, RpcMessage command, long prevLogIndex, long prevLogTerm) {
            this.leaderId = leaderId;
            this.term = term;
            this.command = command;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
        }
    }

    private static class AppendEntriesResponse {
        final String type = "APPEND_ENTRIES_RESPONSE";
        final long term;
        final boolean success;
        final String followerId;

        AppendEntriesResponse(long term, boolean success, String followerId) {
            this.term = term;
            this.success = success;
            this.followerId = followerId;
        }
    }

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
        // Add to the end of the constructor after initializing clusterMembers
        // Initialize the configuration
        Set<ServerInfo> initialServers = new HashSet<>(clusterMembers);
        this.currentConfig = new ClusterConfiguration(initialServers, 0, ClusterConfiguration.ConfigType.OLD);
        this.jointConfig = null;
            
        // Initialize random heartbeat timeout between 5000-7000 milliseconds
        this.heartbeatTimeout = 5000 + random.nextFloat() * 2000;
        this.lastHeartbeatReceived = System.currentTimeMillis();
        
        this.votedFor = null;
        this.receivedVotes = 0;
        this.lastLogIndex = 0;  // Initialize log index to 0
        this.lastLogTerm = 0;
        
        initializeClusterMembers();
        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) {
                this.nodeConnections.put(member, new NodeConnection(member));
            }
            System.out.println("Cluster member: " + member.getHost() + ":" + member.getPort());
        }

        if (nodeType == NodeType.LEADER) {
            System.out.println("Node ini adalah leader dengan log index: " + lastLogIndex);
            startHeartbeat();
        } else {
            System.out.printf("Node ini adalah follower dengan timeout %.2f ms dan log index: %d%n", heartbeatTimeout, lastLogIndex);
            startTimeoutChecker();
        }
    }

    // Add these fields near the top of the RaftNode class
    private ClusterConfiguration currentConfig;
    private ClusterConfiguration jointConfig;
    private boolean inConfigChange = false;
    private final Object configLock = new Object();
    private final Set<String> nonVotingMembers = ConcurrentHashMap.newKeySet();

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
            // OR Option 2: Use container names in vote requests
            votedFor = "raft-node" + port + ":" + port; // Use container name pattern
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

    // Update the handleVoteRequest method
    private void handleVoteRequest(SocketChannel channel, VoteMessage.VoteRequest request) {
        System.out.println("Handling vote request from " + request.getCandidateId());
        boolean voteGranted = false;
        
        // Ignore RequestVote RPCs if received within heartbeat timeout of current leader
        long timeSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeatReceived;
        if (nodeType == NodeType.FOLLOWER && timeSinceLastHeartbeat < heartbeatTimeout / 2) {
            System.out.println("Ignoring vote request - received heartbeat recently (" + 
                            timeSinceLastHeartbeat + "ms ago)");
            
            voteGranted = false;
        } else {
            synchronized (voteLock) {
                // Check if candidate is in current configuration
                String[] candidateParts = request.getCandidateId().split(":");
                if (candidateParts.length == 2) {
                    String host = candidateParts[0];
                    int port;
                    try {
                        port = Integer.parseInt(candidateParts[1]);
                        
                        // Check if server is in current configuration
                        if (!currentConfig.containsServer(host, port)) {
                            System.out.println("Ignoring vote request - server not in configuration");
                            voteGranted = false;
                        } else {
                            // Normal vote logic
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
                    } catch (NumberFormatException e) {
                        voteGranted = false;
                    }
                } else {
                    voteGranted = false;
                }
            }
        }
        
        System.out.println("Voted for: " + votedFor + ", vote granted: " + voteGranted);

        VoteMessage.VoteResponse response = new VoteMessage.VoteResponse(
            currentTerm,
            voteGranted,
            "raft-node" + port + ":" + port
        );

        try {
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
            System.out.println("--------------------------------------------------------------");
            System.out.println("Sent vote response to " + channel.getRemoteAddress());
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
                    } else if (message.contains("\"type\":\"APPEND_ENTRIES\"")) {
                        System.out.println("Received AppendEntries message from " + channel.getRemoteAddress());
                        AppendEntriesMessage appendEntries = gson.fromJson(message, AppendEntriesMessage.class);
                        handleAppendEntries(channel, appendEntries);
                    } else if (message.contains("\"type\":\"APPEND_ENTRIES_RESPONSE\"")) {
                        System.out.println("Received AppendEntries response from " + channel.getRemoteAddress());
                        AppendEntriesResponse response = gson.fromJson(message, AppendEntriesResponse.class);
                        handleAppendEntriesResponse(channel, response);
                    } else if (message.contains("\"type\":\"LOG_REPLICATION\"")) {
                        Map<String, Object> replicationMsg = gson.fromJson(message, Map.class);
                        String commandId = (String) replicationMsg.get("commandId");
                        String followerId = (String) replicationMsg.get("followerId");
                        boolean success = (boolean) replicationMsg.get("success");
                        
                        if (success) {
                            CommandVote vote = commandVotes.get(commandId);
                            if (vote != null) {
                                vote.replicatedNodes.add(followerId);
                                System.out.println("Received log replication confirmation from " + followerId + 
                                    " for command " + commandId + 
                                    " (total replicated: " + vote.replicatedNodes.size() + ")");
                            }
                        }
                        return;
                    } else {
                        System.out.println("MASUK KE PROCESS CLIENT MESSAGEEEEEEEEEEEEEEEEEEEEE !!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                        processClientMessage(channel, message);
                    }
                } catch (Exception e) {
                    System.err.println("Error processing message: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading from channel: " + e.getMessage());
            closeConnection(channel);
        }
    }

    // Update the processClientMessage method
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
                } else {
                    // Process based on method
                    switch (rpcMessage.getMethod()) {
                        case "add_server":
                            Map<String, Object> addParams = (Map<String, Object>) rpcMessage.getParams();
                            String addHost = (String) addParams.get("host");
                            int addPort = ((Number) addParams.get("port")).intValue();
                            response = addServer(addHost, addPort);
                            break;
                            
                        case "remove_server":
                            Map<String, Object> removeParams = (Map<String, Object>) rpcMessage.getParams();
                            String removeHost = (String) removeParams.get("host");
                            int removePort = ((Number) removeParams.get("port")).intValue();
                            response = removeServer(removeHost, removePort);
                            break;
                            
                        case "get_cluster":
                            response = getClusterConfig(rpcMessage.getId());
                            break;
                            
                        default:
                            // Process normal command
                            response = processCommand(rpcMessage);
                            break;
                    }
                }

                // Send response
                sendResponse(clientChannel, response);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
                e.printStackTrace();
            }
        });
    }

    private RpcResponse processCommand(RpcMessage message) {
        if (nodeType != NodeType.LEADER) {
            return new RpcResponse(message.getId(), 
                new RpcResponse.RpcError(-32000, "Not leader", null));
        }

        String method = message.getMethod();
        
        // Handle read-only commands directly
        if (method.equals("ping") || method.equals("get") || method.equals("strln")) {
            System.out.println("Processing read-only command: " + method);
            return executeCommand(message);
        }

        // Only set, append, and del require voting and replication
        if (!method.equals("set") && !method.equals("append") && !method.equals("del")) {
            return new RpcResponse(message.getId(), 
                new RpcResponse.RpcError(-32601, "Method not supported", null));
        }

        // Create a new command vote with current log index
        CommandVote vote = new CommandVote(message, lastLogIndex);
        commandVotes.put(message.getId(), vote);
        System.out.println("Starting vote process for command ID: " + message.getId() + 
            " with current log index: " + lastLogIndex);

        int retryCount = 0;
        final int MAX_RETRIES = 3;
        
        while (retryCount < MAX_RETRIES) {
            // Send AppendEntries to all followers
            for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
                NodeConnection connection = entry.getValue();
                if (connection.isConnected()) {
                    sendAppendEntries(connection, message);
                }
            }

            // Wait for majority of votes or timeout
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < VOTE_TIMEOUT) {
                synchronized (voteLock) {
                    int activeNodes = 1; // Count self
                    for (NodeConnection conn : nodeConnections.values()) {
                        if (conn.isConnected()) {
                            activeNodes++;
                        }
                    }
                    System.out.println("Active nodes: " + activeNodes + 
                        ", Current votes for command " + message.getId() + ": " + vote.votes.size());

                    if (vote.votes.size() >= (activeNodes / 2)) {
                        // We have majority, execute the command
                        System.out.println("Received majority votes for command " + message.getId() + 
                            ", executing command");
                        vote.executed = true;
                        
                        // Execute command and wait for replication
                        RpcResponse response = executeCommand(message);
                        
                        // Increment log index after successful execution
                        lastLogIndex++;
                        System.out.println("Incremented log index to: " + lastLogIndex);
                        
                        // Wait for all followers to replicate
                        System.out.println("Waiting for log replication from all followers for command " + message.getId());
                        startTime = System.currentTimeMillis();
                        while (System.currentTimeMillis() - startTime < VOTE_TIMEOUT) {
                            if (vote.replicatedNodes.size() >= activeNodes - 1) { // All followers have replicated
                                System.out.println("All followers have replicated command " + message.getId());
                                vote.committed = true;
                                return response;
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        
                        // If we got here, some followers didn't replicate in time
                        System.out.println("Warning: Not all followers replicated command " + message.getId() + 
                            " (replicated by " + vote.replicatedNodes.size() + " of " + (activeNodes - 1) + " followers)");
                        return response;
                    }
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            // If we didn't get majority, retry
            retryCount++;
            if (retryCount < MAX_RETRIES) {
                System.out.println("Retrying command " + message.getId() + 
                    " (attempt " + (retryCount + 1) + " of " + MAX_RETRIES + ")");
                // Clear previous votes for this retry
                vote.votes.clear();
                vote.replicatedNodes.clear();
            }
        }

        // If we've exhausted all retries, return timeout error
        System.out.println("Command " + message.getId() + " timed out after " + MAX_RETRIES + 
            " retries. Received " + vote.votes.size() + " votes");
        commandVotes.remove(message.getId());
        return new RpcResponse(message.getId(), 
            new RpcResponse.RpcError(-32006, "Command timed out waiting for votes after " + MAX_RETRIES + " retries", null));
    }

    private RpcResponse executeCommand(RpcMessage message) {
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

            case "config_change":
                // Handle configuration change command
                try {
                    executeConfigChange(params);
                    return new RpcResponse(message.getId(), "Configuration updated");
                } catch (Exception e) {
                    System.err.println("Error applying configuration: " + e.getMessage());
                    e.printStackTrace();
                    return new RpcResponse(message.getId(), 
                        new RpcResponse.RpcError(-32016, "Error applying configuration: " + e.getMessage(), null));
                }
                 
            default:
                return new RpcResponse(message.getId(), 
                    new RpcResponse.RpcError(-32601, "Method not found", null));
        }
    }

    private void handleCommandVote(SocketChannel channel, RpcMessage message) {
        // Add vote for the command
        System.out.println("MASUKKKKKKKKK Handle Command Vote !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
        CommandVote vote = commandVotes.get(message.getId());
        if (vote != null) {
            try {
                String voterId = channel.getRemoteAddress().toString();
                vote.votes.add(voterId);
                System.out.println("Received vote from " + voterId + " for command " + message.getId());
            } catch (IOException e) {
                System.err.println("Error getting remote address for vote: " + e.getMessage());
            }
        } else {
            System.out.println("No vote record found for command " + message.getId());
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

    private void handleMessage(SocketChannel channel, String message) {
        try {
            // Try to parse as RPC message first
            if (message.contains("\"type\":\"APPEND_ENTRIES\"")) {
                AppendEntriesMessage appendEntries = gson.fromJson(message, AppendEntriesMessage.class);
                handleAppendEntries(channel, appendEntries);
                return;
            } else if (message.contains("\"type\":\"APPEND_ENTRIES_RESPONSE\"")) {
                AppendEntriesResponse response = gson.fromJson(message, AppendEntriesResponse.class);
                handleAppendEntriesResponse(channel, response);
                return;
            } else if (message.contains("\"type\":\"LOG_REPLICATION\"")) {
                Map<String, Object> replicationMsg = gson.fromJson(message, Map.class);
                String commandId = (String) replicationMsg.get("commandId");
                String followerId = (String) replicationMsg.get("followerId");
                boolean success = (boolean) replicationMsg.get("success");
                
                if (success) {
                    CommandVote vote = commandVotes.get(commandId);
                    if (vote != null) {
                        vote.replicatedNodes.add(followerId);
                        System.out.println("Received log replication confirmation from " + followerId + 
                            " for command " + commandId + 
                            " (total replicated: " + vote.replicatedNodes.size() + ")");
                    }
                }
                return;
            }

            RpcMessage rpcMessage = RpcMessage.fromJson(message);
            System.out.println("Received message: " + message);
            
            // Handle other RPC messages
            switch (rpcMessage.getMethod()) {
                case "ping":
                    sendResponse(channel, new RpcResponse(rpcMessage.getId(), "pong"));
                    break;
                    
                case "get":
                case "set":
                case "del":
                case "append":
                case "strln":
                    if (nodeType == NodeType.LEADER) {
                        RpcResponse response = processCommand(rpcMessage);
                        sendResponse(channel, response);
                    } else {
                        // If not leader, forward to leader
                        ServerInfo leader = clusterMembers.stream()
                            .filter(s -> s.getType() == NodeType.LEADER)
                            .findFirst()
                            .orElse(null);

                        if (leader != null) {
                            NodeConnection leaderConn = nodeConnections.get(leader);
                            if (leaderConn != null) {
                                try {
                                    leaderConn.send(gson.toJson(rpcMessage));
                                } catch (IOException e) {
                                    System.err.println("Error forwarding command to leader: " + e.getMessage());
                                }
                            }
                        }
                    }
                    break;
                    
                default:
                    sendResponse(channel, new RpcResponse(rpcMessage.getId(),
                        new RpcResponse.RpcError(-32601, "Method not found", null)));
            }
        } catch (Exception e) {
            System.err.println("Error handling message: " + e.getMessage());
        }
    }

    private void sendAppendEntries(NodeConnection connection, RpcMessage command) {
        try {
            AppendEntriesMessage appendEntries = new AppendEntriesMessage(
                address.getHostAddress() + ":" + port,
                currentTerm,
                command,
                lastLogIndex,  // Send current log index
                lastLogTerm
            );
            String jsonMessage = gson.toJson(appendEntries);
            System.out.println("Sending AppendEntries to " + connection.getServerInfo() + 
                " for command ID: " + command.getId() + 
                " with log index: " + lastLogIndex);
            connection.send(jsonMessage);
        } catch (IOException e) {
            System.err.println("Error sending AppendEntries: " + e.getMessage());
        }
    }

    private void handleAppendEntries(SocketChannel channel, AppendEntriesMessage message) {
        System.out.println("Processing AppendEntries from " + message.leaderId + 
            " for command ID: " + message.command.getId() + 
            " with prevLogIndex: " + message.prevLogIndex);
        
        // Update term if necessary
        if (message.term > currentTerm) {
            currentTerm = message.term;
            nodeType = NodeType.FOLLOWER;
            votedFor = null;
        }

        // Check if we can accept the entries
        boolean success = true;
        if (message.prevLogIndex > lastLogIndex) {
            System.out.println("Log index mismatch: leader=" + message.prevLogIndex + ", follower=" + lastLogIndex);
            success = false;
        }

        // Send response
        AppendEntriesResponse response = new AppendEntriesResponse(
            currentTerm,
            success,
            address.getHostAddress() + ":" + port
        );

        try {
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
            System.out.println("Sent AppendEntries response: " + jsonResponse + 
                " for command ID: " + message.command.getId() + 
                " (success: " + success + ")");

            // If successful, process the command and replicate the log
            if (success && message.command != null) {
                System.out.println("Processing command from AppendEntries with ID: " + message.command.getId());
                RpcResponse rpcResponse = executeCommand(message.command);
                
                // Update our log index
                lastLogIndex = message.prevLogIndex + 1;
                System.out.println("Updated log index to: " + lastLogIndex);
                
                // Send replication confirmation to leader
                Map<String, Object> replicationMsg = new HashMap<>();
                replicationMsg.put("type", "LOG_REPLICATION");
                replicationMsg.put("commandId", message.command.getId());
                replicationMsg.put("followerId", address.getHostAddress() + ":" + port);
                replicationMsg.put("success", true);
                replicationMsg.put("logIndex", lastLogIndex);
                
                String replicationJson = gson.toJson(replicationMsg);
                buffer = ByteBuffer.wrap((replicationJson + "\n").getBytes());
                channel.write(buffer);
                System.out.println("Sent log replication confirmation for command " + message.command.getId() + 
                    " with logIndex: " + lastLogIndex);
                
                // Send the response back to the leader
                sendResponse(channel, rpcResponse);
            }
        } catch (IOException e) {
            System.err.println("Error sending AppendEntries response: " + e.getMessage());
        }
    }

    private void handleAppendEntriesResponse(SocketChannel channel, AppendEntriesResponse response) {
        System.out.println("Processing AppendEntries response from " + response.followerId);
        
        if (response.term > currentTerm) {
            currentTerm = response.term;
            nodeType = NodeType.FOLLOWER;
            votedFor = null;
            return;
        }

        if (response.success) {
            // Find the command vote by follower ID
            for (Map.Entry<String, CommandVote> entry : commandVotes.entrySet()) {
                CommandVote vote = entry.getValue();
                if (!vote.executed) {
                    vote.votes.add(response.followerId);
                    System.out.println("Added vote from " + response.followerId + " for command ID: " + entry.getKey());
                    System.out.println("Current votes for command " + entry.getKey() + ": " + vote.votes.size());
                    break;
                }
            }
        } else {
            System.out.println("Received unsuccessful AppendEntries response from " + response.followerId);
        }
    }

    /**
     * Adds a new server to the cluster using the joint consensus protocol.
     */
    public synchronized RpcResponse addServer(String host, int port) {
        if (nodeType != NodeType.LEADER) {
            return new RpcResponse("add_server", 
                new RpcResponse.RpcError(-32000, "Not leader", null));
        }
        
        synchronized (configLock) {
            if (inConfigChange) {
                return new RpcResponse("add_server", 
                    new RpcResponse.RpcError(-32010, "Configuration change already in progress", null));
            }
            
            // Check if server already exists
            if (currentConfig.containsServer(host, port)) {
                return new RpcResponse("add_server", 
                    new RpcResponse.RpcError(-32011, "Server already exists in configuration", null));
            }
            
            try {
                // Mark that we're in a configuration change
                inConfigChange = true;
                
                // First, add as non-voting member to catch up
                ServerInfo newServer = new ServerInfo(host, port, NodeType.FOLLOWER);
                nonVotingMembers.add(host + ":" + port);
                
                // Create connection to new server
                nodeConnections.put(newServer, new NodeConnection(newServer));
                
                System.out.println("Added " + host + ":" + port + " as non-voting member to catch up");
                
                // Wait for the new server to catch up (replicate entries)
                boolean caughtUp = waitForServerCatchUp(newServer);
                if (!caughtUp) {
                    nonVotingMembers.remove(host + ":" + port);
                    nodeConnections.remove(newServer);
                    inConfigChange = false;
                    return new RpcResponse("add_server", 
                        new RpcResponse.RpcError(-32012, "Timed out waiting for server to catch up", null));
                }
                
                // Server has caught up, remove from non-voting members
                nonVotingMembers.remove(host + ":" + port);
                
                // Create joint configuration (Cold,new)
                Set<ServerInfo> newCluster = currentConfig.getServers();
                newCluster.add(newServer);
                
                // Add to visible members list
                clusterMembers.add(newServer);
                
                // Create and replicate joint consensus configuration
                long newConfigIndex = lastLogIndex + 1;
                jointConfig = new ClusterConfiguration(newCluster, newConfigIndex, ClusterConfiguration.ConfigType.JOINT);
                
                // Log and commit the joint configuration
                logConfigurationChange(jointConfig);
                
                // Wait for the joint configuration to be committed
                if (!waitForConfigCommit(jointConfig.getConfigIndex(), 10000)) {
                    inConfigChange = false;
                    return new RpcResponse("add_server", 
                        new RpcResponse.RpcError(-32013, "Failed to commit joint configuration", null));
                }
                
                // Now create the new configuration (Cnew)
                newConfigIndex = lastLogIndex + 1;
                ClusterConfiguration newConfig = new ClusterConfiguration(newCluster, newConfigIndex, ClusterConfiguration.ConfigType.NEW);
                
                // Log and commit the new configuration
                logConfigurationChange(newConfig);
                
                // Wait for new configuration to be committed
                if (!waitForConfigCommit(newConfig.getConfigIndex(), 10000)) {
                    inConfigChange = false;
                    return new RpcResponse("add_server", 
                        new RpcResponse.RpcError(-32014, "Failed to commit new configuration", null));
                }
                
                // Update the current configuration
                currentConfig = newConfig;
                jointConfig = null;
                inConfigChange = false;
                
                System.out.println("Server " + host + ":" + port + " successfully added to the cluster");
                return new RpcResponse("add_server", "Server added successfully");
                
            } catch (Exception e) {
                inConfigChange = false;
                System.err.println("Error adding server: " + e.getMessage());
                e.printStackTrace();
                return new RpcResponse("add_server", 
                    new RpcResponse.RpcError(-32015, "Error adding server: " + e.getMessage(), null));
            }
        }
    }

    /**
     * Removes a server from the cluster using the joint consensus protocol.
     */
    public synchronized RpcResponse removeServer(String host, int port) {
        if (nodeType != NodeType.LEADER) {
            return new RpcResponse("remove_server", 
                new RpcResponse.RpcError(-32000, "Not leader", null));
        }
        
        synchronized (configLock) {
            if (inConfigChange) {
                return new RpcResponse("remove_server", 
                    new RpcResponse.RpcError(-32010, "Configuration change already in progress", null));
            }
            
            // Find the server to remove
            ServerInfo serverToRemove = currentConfig.getServer(host, port);
            if (serverToRemove == null) {
                return new RpcResponse("remove_server", 
                    new RpcResponse.RpcError(-32011, "Server not found in configuration", null));
            }
            
            // Don't allow removing the current leader (self)
            if (host.equals(address.getHostName()) && port == this.port) {
                return new RpcResponse("remove_server", 
                    new RpcResponse.RpcError(-32012, "Cannot remove current leader", null));
            }
            
            try {
                // Mark that we're in a configuration change
                inConfigChange = true;
                
                // Create joint configuration (Cold,new) without the server
                Set<ServerInfo> newCluster = currentConfig.getServers();
                newCluster.removeIf(s -> s.getHost().equals(host) && s.getPort() == port);
                
                // Create and replicate joint consensus configuration
                long newConfigIndex = lastLogIndex + 1;
                jointConfig = new ClusterConfiguration(newCluster, newConfigIndex, ClusterConfiguration.ConfigType.JOINT);
                
                // Log and commit the joint configuration
                logConfigurationChange(jointConfig);
                
                // Wait for the joint configuration to be committed
                if (!waitForConfigCommit(jointConfig.getConfigIndex(), 10000)) {
                    inConfigChange = false;
                    return new RpcResponse("remove_server", 
                        new RpcResponse.RpcError(-32013, "Failed to commit joint configuration", null));
                }
                
                // Now create the new configuration (Cnew)
                newConfigIndex = lastLogIndex + 1;
                ClusterConfiguration newConfig = new ClusterConfiguration(newCluster, newConfigIndex, ClusterConfiguration.ConfigType.NEW);
                
                // Log and commit the new configuration
                logConfigurationChange(newConfig);
                
                // Wait for new configuration to be committed
                if (!waitForConfigCommit(newConfig.getConfigIndex(), 10000)) {
                    inConfigChange = false;
                    return new RpcResponse("remove_server", 
                        new RpcResponse.RpcError(-32014, "Failed to commit new configuration", null));
                }
                
                // Update the current configuration
                currentConfig = newConfig;
                
                // Remove connection and from cluster members
                nodeConnections.remove(serverToRemove);
                clusterMembers.removeIf(s -> s.getHost().equals(host) && s.getPort() == port);
                
                // Clear joint config
                jointConfig = null;
                inConfigChange = false;
                
                System.out.println("Server " + host + ":" + port + " successfully removed from the cluster");
                return new RpcResponse("remove_server", "Server removed successfully");
                
            } catch (Exception e) {
                inConfigChange = false;
                System.err.println("Error removing server: " + e.getMessage());
                e.printStackTrace();
                return new RpcResponse("remove_server", 
                    new RpcResponse.RpcError(-32015, "Error removing server: " + e.getMessage(), null));
            }
        }
    }

    /**
     * Logs a configuration change entry to the Raft log.
     */
    private void logConfigurationChange(ClusterConfiguration config) {
        // Create configuration entry
        Map<String, Object> configEntry = new HashMap<>();
        configEntry.put("type", "config_change");
        configEntry.put("config_index", config.getConfigIndex());
        configEntry.put("config_id", config.getConfigId());
        configEntry.put("config_type", config.getType().toString());
        
        // Add server information
        Set<Map<String, Object>> serverInfos = new HashSet<>();
        for (ServerInfo server : config.getServers()) {
            Map<String, Object> serverData = new HashMap<>();
            serverData.put("host", server.getHost());
            serverData.put("port", server.getPort());
            serverData.put("node_type", server.getType().toString());
            serverInfos.add(serverData);
        }
        configEntry.put("servers", serverInfos);
        
        // Create command
        String cmdId = "config_" + config.getConfigIndex() + "_" + System.currentTimeMillis();
        RpcMessage configCommand = new RpcMessage(cmdId, "config_change", configEntry);
        
        // Add to command votes
        CommandVote vote = new CommandVote(configCommand, config.getConfigIndex());
        commandVotes.put(configCommand.getId(), vote);
        
        System.out.println("Logging configuration change: " + config.getType() + 
                        " at index " + config.getConfigIndex() + 
                        " with " + config.getServers().size() + " servers");
        
        // Replicate to all servers in both configurations
        for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
            NodeConnection connection = entry.getValue();
            if (connection.isConnected()) {
                sendAppendEntries(connection, configCommand);
            }
        }
        
        // Execute locally
        executeConfigChange(configEntry);
    }

    /**
     * Waits for a configuration to be committed.
     */
    private boolean waitForConfigCommit(long configIndex, long timeoutMs) {
        long startTime = System.currentTimeMillis();
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            // Configuration is committed when majority of both old and new configs have it
            CommandVote vote = null;
            
            // Find the vote for this config index
            for (CommandVote v : commandVotes.values()) {
                if (v.logIndex == configIndex) {
                    vote = v;
                    break;
                }
            }
            
            if (vote != null) {
                // For joint consensus, we need majority in both old and new configurations
                Set<ServerInfo> servers = currentConfig.getServers();
                if (jointConfig != null) {
                    servers.addAll(jointConfig.getServers());
                }
                
                int totalServers = servers.size();
                int replicationCount = vote.replicatedNodes.size() + 1; // +1 for self
                
                if (replicationCount > totalServers / 2) {
                    System.out.println("Configuration at index " + configIndex + " committed with " + 
                                    replicationCount + "/" + totalServers + " servers");
                    return true;
                }
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        System.out.println("Timed out waiting for configuration at index " + configIndex + " to be committed");
        return false;
    }

    /**
     * Executes a configuration change locally.
     */
    private void executeConfigChange(Map<String, Object> configEntry) {
        String configType = (String) configEntry.get("config_type");
        long configIndex = ((Number) configEntry.get("config_index")).longValue();
        String configId = (String) configEntry.get("config_id");
        
        // Parse servers
        Set<ServerInfo> servers = new HashSet<>();
        Set<Map<String, Object>> serverInfos = (Set<Map<String, Object>>) configEntry.get("servers");
        
        for (Map<String, Object> serverData : serverInfos) {
            String host = (String) serverData.get("host");
            int port = ((Number) serverData.get("port")).intValue();
            NodeType type = NodeType.valueOf((String) serverData.get("node_type"));
            servers.add(new ServerInfo(host, port, type));
        }
        
        // Create configuration object
        ClusterConfiguration.ConfigType type = ClusterConfiguration.ConfigType.valueOf(configType);
        ClusterConfiguration newConfig = new ClusterConfiguration(servers, configIndex, type);
        
        // Apply configuration based on type
        if (type == ClusterConfiguration.ConfigType.JOINT) {
            // Apply joint configuration
            jointConfig = newConfig;
            System.out.println("Applied joint configuration at index " + configIndex);
        } else if (type == ClusterConfiguration.ConfigType.NEW) {
            // Apply new configuration
            currentConfig = newConfig;
            jointConfig = null;
            System.out.println("Applied new configuration at index " + configIndex);
            
            // If leader is not in new config, step down
            boolean leaderInConfig = false;
            for (ServerInfo server : servers) {
                if (server.getHost().equals(address.getHostName()) && server.getPort() == port) {
                    leaderInConfig = true;
                    break;
                }
            }
            
            if (!leaderInConfig && nodeType == NodeType.LEADER) {
                System.out.println("Leader not in new configuration, stepping down");
                nodeType = NodeType.FOLLOWER;
            }
        } else {
            // Apply old configuration (rare case)
            currentConfig = newConfig;
            System.out.println("Applied old configuration at index " + configIndex);
        }
        
        // Update log index if necessary
        if (configIndex > lastLogIndex) {
            lastLogIndex = configIndex;
        }
    }

    /**
     * Waits for a server to catch up with the leader's log.
     */
    private boolean waitForServerCatchUp(ServerInfo newServer) {
        NodeConnection connection = nodeConnections.get(newServer);
        if (connection == null || !connection.isConnected()) {
            try {
                if (connection == null) {
                    connection = new NodeConnection(newServer);
                    nodeConnections.put(newServer, connection);
                }
                connection.connect();
            } catch (IOException e) {
                System.err.println("Failed to connect to new server during catch-up: " + e.getMessage());
                return false;
            }
        }
        
        // Current leader's log state
        long targetIndex = lastLogIndex;
        
        // Transfer all log entries to the new server
        for (long i = 0; i <= targetIndex; i++) {
            CommandVote vote = null;
            
            // Find command for this log index
            for (CommandVote v : commandVotes.values()) {
                if (v.logIndex == i) {
                    vote = v;
                    break;
                }
            }
            
            if (vote != null) {
                try {
                    // Send AppendEntries with this log entry
                    sendAppendEntries(connection, vote.command);
                    
                    // Wait briefly for replication
                    Thread.sleep(100);
                } catch (Exception e) {
                    System.err.println("Error sending log entry to new server: " + e.getMessage());
                    return false;
                }
            }
        }
        
        // Wait for server to acknowledge replication
        long timeout = System.currentTimeMillis() + 10000; // 10 second timeout
        boolean caughtUp = false;
        
        while (System.currentTimeMillis() < timeout) {
            // Check if server has replicated all entries
            boolean allReplicated = true;
            
            for (CommandVote vote : commandVotes.values()) {
                if (vote.logIndex <= targetIndex) {
                    String serverKey = newServer.getHost() + ":" + newServer.getPort();
                    if (!vote.replicatedNodes.contains(serverKey)) {
                        allReplicated = false;
                        break;
                    }
                }
            }
            
            if (allReplicated) {
                caughtUp = true;
                break;
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        System.out.println("New server catch-up completed: " + caughtUp);
        return caughtUp;
    }

    /**
     * Returns the current cluster configuration.
     */
    public RpcResponse getClusterConfig(String requestId) {
        Map<String, Object> configInfo = new HashMap<>();
        
        configInfo.put("current_config_index", currentConfig.getConfigIndex());
        configInfo.put("current_config_type", currentConfig.getType().toString());
        
        List<Map<String, Object>> serversList = new ArrayList<>();
        for (ServerInfo server : currentConfig.getServers()) {
            Map<String, Object> serverInfo = new HashMap<>();
            serverInfo.put("host", server.getHost());
            serverInfo.put("port", server.getPort());
            serverInfo.put("type", server.getType().toString());
            
            // Check if connection is active
            NodeConnection connection = nodeConnections.get(server);
            boolean connected = connection != null && connection.isConnected();
            serverInfo.put("connected", connected);
            
            // Mark if this is the current node
            boolean isSelf = server.getHost().equals(address.getHostName()) && server.getPort() == port;
            serverInfo.put("self", isSelf);
            
            // Mark if this is a non-voting member
            boolean isNonVoting = nonVotingMembers.contains(server.getHost() + ":" + server.getPort());
            serverInfo.put("non_voting", isNonVoting);
            
            serversList.add(serverInfo);
        }
        configInfo.put("servers", serversList);
        
        // Add joint config info if present
        if (jointConfig != null) {
            configInfo.put("joint_config_index", jointConfig.getConfigIndex());
            configInfo.put("joint_config_type", jointConfig.getType().toString());
            
            List<Map<String, Object>> jointServersList = new ArrayList<>();
            for (ServerInfo server : jointConfig.getServers()) {
                Map<String, Object> serverInfo = new HashMap<>();
                serverInfo.put("host", server.getHost());
                serverInfo.put("port", server.getPort());
                serverInfo.put("type", server.getType().toString());
                
                NodeConnection connection = nodeConnections.get(server);
                boolean connected = connection != null && connection.isConnected();
                serverInfo.put("connected", connected);
                
                boolean isSelf = server.getHost().equals(address.getHostName()) && server.getPort() == port;
                serverInfo.put("self", isSelf);
                
                boolean isNonVoting = nonVotingMembers.contains(server.getHost() + ":" + server.getPort());
                serverInfo.put("non_voting", isNonVoting);
                
                jointServersList.add(serverInfo);
            }
            configInfo.put("joint_servers", jointServersList);
        }
        
        return new RpcResponse(requestId, configInfo);
    }
}
