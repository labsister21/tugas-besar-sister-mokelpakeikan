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
import com.raft.rpc.AppendEntriesMessage;
import com.raft.rpc.AppendEntriesResponse;
import com.raft.rpc.CommandVote;
import com.raft.storage.PersistentState;
import com.raft.storage.PersistentState.State;
// import com.raft.node.LogEntry;

public class RaftNode {
    // Persistent State
    private long currentTerm = 0;
    private String votedFor = null;
    private final List<LogEntry> logEntries = new ArrayList<>();

    // Volatile State
    private int commitIndex = 0;
    private int lastApplied = 0;

    // Volatile State (Leader)
    private List<Integer> nextIndex = null;
    private List<Integer> matchIndex = null;


    // Connections
    private final InetAddress address;
    private final int port;
    private ServerSocketChannel serverSocket;
    private final Map<SocketChannel, ByteBuffer> clientBuffers;
    private final Map<ServerInfo, NodeConnection> nodeConnections;


    // Timeouts / Intervals
    private final int VOTE_TIMEOUT = 5000; // 5 seconds timeout for votes
    private final float heartbeatTimeout;
    private static final long HEARTBEAT_INTERVAL = 5000; // 2 seconds


    // Service
    private final ExecutorService executorService;
    private final ScheduledExecutorService heartbeatExecutor;

    // Cluster Information
    private final List<ServerInfo> clusterMembers;


    // Server Information
    private NodeType nodeType;
    private final Map<String, String> dataStore;
    private volatile boolean running;
    private long lastHeartbeatReceived;
    private int receivedVotes;
    private long lastLogIndex;
    private long lastLogTerm;
    private boolean inElection;
    private List<ServerInfo> currentConfig;
    private List<ServerInfo> jointConfig;
    private List<ServerInfo> newConfig;
    private boolean isJointConsensus = false;
    private long configChangeIndex = -1;
    private final Map<String, Boolean> configVotes = new ConcurrentHashMap<>();

    // Constants
    private static final Random random = new Random();
    private final Gson gson;
    private final Object voteLock = new Object();
    private final Map<String, CommandVote> commandVotes = new ConcurrentHashMap<>();
    private Selector selector;



    // SINIII

    private static class ConfigurationChange {
        final String type = "CONFIG_CHANGE";
        final List<ServerInfo> oldConfig;
        final List<ServerInfo> newConfig;
        final boolean isJointConsensus;
        final long term;
        final String leaderId;
        final long configIndex;
        
        ConfigurationChange(List<ServerInfo> oldConfig, List<ServerInfo> newConfig, boolean isJointConsensus, 
                            long term, String leaderId, long configIndex) {
            this.oldConfig = oldConfig;
            this.newConfig = newConfig;
            this.isJointConsensus = isJointConsensus;
            this.term = term;
            this.leaderId = leaderId;
            this.configIndex = configIndex;
        }
    }

    private static class ConfigChangeResponse {
        final String type = "CONFIG_CHANGE_RESPONSE";
        final boolean success;
        final long term;
        final String nodeId;
        
        ConfigChangeResponse(boolean success, long term, String nodeId) {
            this.success = success;
            this.term = term;
            this.nodeId = nodeId;
        }
    }

    // SAMPE SINIII


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
        this.gson = new Gson();

        this.currentConfig = new ArrayList<>(clusterMembers);
        this.jointConfig = null;
        this.newConfig = null;
        
        // Load persistent state
        State state = PersistentState.loadState();
        this.currentTerm = state.getCurrentTerm();
        this.votedFor = state.getVotedFor();
        this.logEntries.addAll(state.getLog());
        
        // Initialize random heartbeat timeout between 5000-7000 milliseconds
        this.heartbeatTimeout = 5000 + random.nextFloat() * 2000;
        this.lastHeartbeatReceived = System.currentTimeMillis();
        
        this.receivedVotes = 0;
        this.lastLogIndex = logEntries.size();  // Initialize log index based on loaded log
        this.lastLogTerm = logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getTerm();
        
        
        initializeClusterMembers();

        this.currentConfig = new ArrayList<>(clusterMembers);
        this.jointConfig = null;
        this.newConfig = null;

        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) {
                this.nodeConnections.put(member, new NodeConnection(member));
            }
            System.out.println("Cluster member: " + member.getHost() + ":" + member.getPort());
        }

        startTimeoutChecker();
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
                System.out.println("Received heartbeat from leader: " + heartbeat.getLeaderId() + 
                                 " (term: " + heartbeat.getTerm() + " heartbeat got on " + (now-this.lastHeartbeatReceived) + " , timestamp : "+now+")");
            }
            lastHeartbeatReceived = System.currentTimeMillis();
            
            // Update term if necessary
            if (heartbeat.getTerm() > currentTerm) {
                currentTerm = heartbeat.getTerm();
                votedFor = null;
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                if (nodeType == NodeType.CANDIDATE) {
                    cancelCandidation();
                }
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
            PersistentState.saveState(currentTerm, votedFor, logEntries);
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
            if (nodeType == NodeType.CANDIDATE) {
                System.out.println("Election timeout - reverting to follower");
                cancelCandidation();
            }
        }, 10000, MILLISECONDS);
    }

    private void cancelCandidation() {
        synchronized (voteLock) {
            if (inElection) {
                inElection = false;
                nodeType = NodeType.FOLLOWER;
                votedFor = null;
            }
        }
    }

    private void sendVoteRequest(NodeConnection connection, VoteMessage.VoteRequest request) {
        try {
            String jsonRequest = gson.toJson(request);
            connection.send(jsonRequest);
            
            // Register channel untuk membaca response
            SocketChannel channel = connection.getChannel();
            channel.configureBlocking(false);
            SelectionKey key = channel.keyFor(selector);
            
            clientBuffers.putIfAbsent(channel, ByteBuffer.allocate(1024));
            if (key == null || !key.isValid()) {
                key = channel.register(selector, SelectionKey.OP_READ);
                System.out.println("Registered new channel for reading vote responses");
            } else {
                key.interestOps(SelectionKey.OP_READ);
                System.out.println("Updated existing channel for reading vote responses");
            }
            selector.wakeup();
        } catch (Exception e) {
            System.err.println("Error sending vote request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleVoteRequest(SocketChannel channel, VoteMessage.VoteRequest request) {
        boolean voteGranted = false;
        synchronized (voteLock) {
            if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                votedFor = null;
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                revertToFollower();
            }

            if (request.getTerm() == currentTerm && 
                (votedFor == null || votedFor.equals(request.getCandidateId()))) {
                votedFor = request.getCandidateId();
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                voteGranted = true;
            }
        }
        System.out.println("voted for : " + votedFor);

        VoteMessage.VoteResponse response = new VoteMessage.VoteResponse(
            currentTerm,
            voteGranted,
            address.getHostAddress() + ":" + port
        );

        long now = System.currentTimeMillis();
        System.out.println("created vote response for " + request.getCandidateId() + " response : " + voteGranted + " at " + now/1000 + " s with response type : " + response.getType());

        try {
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
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
            cancelCandidation();
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

    private void revertToFollower() {
        nodeType = NodeType.FOLLOWER;
        votedFor = null;
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
                    SelectionKey key = keys.next();
                    keys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    if (key.isAcceptable()) {
                        accept(key);
                    } else if (key.isReadable()) {
                        read(key);
                    }else if (key.isConnectable()) {
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
            if (nc.getChannel() == channel) { // Perbandingan referensi
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

        if (buffer == null) {
            System.err.println("No buffer found for channel: " + channel);
            return;
        }
        
        try {
            int bytesRead = channel.read(buffer);
            System.out.println("Read " + bytesRead + " bytes from " + channel.getRemoteAddress());
            if (bytesRead == -1) {
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
                System.out.println("message recieved from " + channel.getRemoteAddress() + " : " + message);
                // Parse message to determine type
                try {
                    if (message.contains("\"type\":\"HEARTBEAT\"")) {
                        handleHeartbeat(message);
                    } else if (message.contains("\"type\":\"VOTE_REQUEST\"")) {
                        inElection = true;
                        System.out.println("Received vote request from " + channel.getRemoteAddress());
                        VoteMessage.VoteRequest voteRequest = gson.fromJson(message, VoteMessage.VoteRequest.class);
                        handleVoteRequest(channel, voteRequest);
                    } else if (message.contains("\"type\":\"VOTE_RESPONSE\"")) {
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
                    } else if (message.contains("\"type\":\"CONFIG_CHANGE\"")) {
                        System.out.println("Received configuration change message");
                        ConfigurationChange configChange = gson.fromJson(message, ConfigurationChange.class);
                        handleConfigurationChange(channel, configChange);
                    } else if (message.contains("\"type\":\"CONFIG_CHANGE_RESPONSE\"")) {
                        System.out.println("Received configuration change response");
                        ConfigChangeResponse configResponse = gson.fromJson(message, ConfigChangeResponse.class);
                        handleConfigurationResponse(configResponse);
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

    private void processClientMessage(SocketChannel clientChannel, String message) {
        executorService.submit(() -> {
            try {
                RpcMessage rpcMessage = RpcMessage.fromJson(message);
                RpcResponse response;
                
                // Handle getLog command directly without leader check
                if (rpcMessage.getMethod().toLowerCase().equals("getlog")) {
                    response = executeCommand(rpcMessage);
                } else if (nodeType != NodeType.LEADER) {
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
                    // Process the command if this is the leader
                    response = processCommand(rpcMessage);
                }

                // Send response
                sendResponse(clientChannel, response);
            } catch (Exception e) {
                System.err.println("Error processing message: " + e.getMessage());
            }
        });
    }

    private RpcResponse processCommand(RpcMessage message) {
        if (nodeType != NodeType.LEADER) {
            return new RpcResponse(message.getId(), 
                new RpcResponse.RpcError(-32000, "Not leader", null));
        }

        String method = message.getMethod().toLowerCase(); 
        System.out.println("method : "+method);
        // Handle read-only commands directly
        if (method.equals("ping") || method.equals("get") || method.equals("strln")) {
            System.out.println("Processing read-only command: " + method);
            return executeCommand(message);
        }

        // Handle cluster membership changes
        if (method.equals("addserver") || method.equals("removeserver")) {
            System.out.println("Processing membership change: " + method);
            Map<String, Object> params = (Map<String, Object>) message.getParams();
            String host = (String) params.get("host");
            int port = ((Number) params.get("port")).intValue();
            
            if (method.equals("addserver")) {
                return addServer(host, port);
            } else {
                return removeServer(host, port);
            }
        }

        // Only set, append, and del require voting and replication
        if (!method.equals("set") && !method.equals("append") && !method.equals("del")) {
            return new RpcResponse(message.getId(), 
                new RpcResponse.RpcError(-32601, "Method not supported in", null));
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
                        
                        // Add log entry
                        logEntries.add(new LogEntry(
                            message.getMethod(),
                            message.getId(),
                            lastLogIndex + 1,
                            currentTerm,
                            (Map<String, Object>) message.getParams()
                        ));
                        
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
        String method = message.getMethod().toLowerCase(); // Convert to lowercase for case-insensitive comparison
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

            case "getlog":  
                List<Map<String, Object>> logList = new ArrayList<>();
                for (LogEntry entry : logEntries) {
                    Map<String, Object> logMap = new HashMap<>();
                    logMap.put("index", entry.getLogIndex());
                    logMap.put("term", entry.getTerm());
                    logMap.put("method", entry.getMethod());
                    logMap.put("commandId", entry.getCommandId());
                    logMap.put("params", entry.getParams());
                    logList.add(logMap);
                }
                return new RpcResponse(message.getId(), logList);
                 
            default:
                return new RpcResponse(message.getId(), 
                    new RpcResponse.RpcError(-32601, "Method not found", null));
        }
    }

    private void printLogEntries() {
        System.out.println("\n=== Log Entries ===");
        if (logEntries.isEmpty()) {
            System.out.println("No log entries yet");
        } else {
            for (LogEntry entry : logEntries) {
                System.out.println(entry);
            }
        }
        System.out.println("=================\n");
    }

    private void handleCommandVote(SocketChannel channel, RpcMessage message) {
        // Add vote for the command
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
        synchronized (voteLock) {
            if (message.getTerm() > currentTerm) {
                currentTerm = message.getTerm();
                votedFor = null;
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                revertToFollower();
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
                    
                    // Add log entry
                    logEntries.add(new LogEntry(
                        message.command.getMethod(),
                        message.command.getId(),
                        message.prevLogIndex + 1,
                        currentTerm,
                        (Map<String, Object>) message.command.getParams()
                    ));
                    
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

                    // Save state after adding new log entry
                    PersistentState.saveState(currentTerm, votedFor, logEntries);
                }
            } catch (IOException e) {
                System.err.println("Error sending AppendEntries response: " + e.getMessage());
            }
        }
    }

    private void handleAppendEntriesResponse(SocketChannel channel, AppendEntriesResponse response) {
        System.out.println("Processing AppendEntries response from " + response.followerId);
        
        if (response.term > currentTerm) {
            currentTerm = response.term;
            votedFor = null;
            PersistentState.saveState(currentTerm, votedFor, logEntries);
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

    // SINIII

    public RpcResponse addServer(String host, int port) {
        if (nodeType != NodeType.LEADER) {
            ServerInfo leader = clusterMembers.stream()
                .filter(s -> s.getType() == NodeType.LEADER)
                .findFirst()
                .orElse(null);
                
            if (leader != null) {
                return new RpcResponse("addServer", 
                    new RpcResponse.RpcError(-32000, "Not leader", 
                        Map.of("leader_host", leader.getHost(), "leader_port", leader.getPort())));
            } else {
                return new RpcResponse("addServer",
                    new RpcResponse.RpcError(-32001, "Leader not found", null));
            }
        }
        
        // Check if server already exists
        boolean serverExists = clusterMembers.stream()
            .anyMatch(s -> s.getHost().equals(host) && s.getPort() == port);
            
        if (serverExists) {
            return new RpcResponse("addServer", 
                new RpcResponse.RpcError(-32002, "Server already exists in cluster", null));
        }
        
        // Check if configuration change is already in progress
        if (isJointConsensus || jointConfig != null || newConfig != null) {
            return new RpcResponse("addServer", 
                new RpcResponse.RpcError(-32003, "Configuration change already in progress", null));
        }
        
        // Create new server info
        ServerInfo newServer = new ServerInfo(host, port, NodeType.FOLLOWER);
        
        // First, add as non-voting member to catch up
        System.out.println("Adding server " + host + ":" + port + " as non-voting member");
        
        // Create connection to new server
        NodeConnection connection = new NodeConnection(newServer);
        try {
            connection.connect();
            nodeConnections.put(newServer, connection);
        } catch (IOException e) {
            System.err.println("Failed to connect to new server: " + e.getMessage());
            return new RpcResponse("addServer", 
                new RpcResponse.RpcError(-32004, "Failed to connect to new server", null));
        }
        
        // Start replicating logs to the new server (non-voting)
        replicateLogsToNewServer(connection);
        
        // Create new configuration that includes the new server
        newConfig = new ArrayList<>(currentConfig);
        newConfig.add(newServer);
        
        // Start the two-phase configuration change
        startJointConsensus();
        
        return new RpcResponse("addServer", "Server addition initiated");
    }

    private void replicateLogsToNewServer(NodeConnection connection) {
        try {
            connection.send(gson.toJson(Map.of(
                "type", "SYNC_LOGS",
                "leaderId", address.getHostAddress() + ":" + port,
                "term", currentTerm,
                "lastLogIndex", lastLogIndex,
                "lastLogTerm", lastLogTerm
            )));
            System.out.println("Started log replication to new server");
        } catch (IOException e) {
            System.err.println("Error replicating logs to new server: " + e.getMessage());
        }
    }

    // Method to remove a server from the cluster
    public RpcResponse removeServer(String host, int port) {
        if (nodeType != NodeType.LEADER) {
            ServerInfo leader = clusterMembers.stream()
                .filter(s -> s.getType() == NodeType.LEADER)
                .findFirst()
                .orElse(null);
                
            if (leader != null) {
                return new RpcResponse("removeServer", 
                    new RpcResponse.RpcError(-32000, "Not leader", 
                        Map.of("leader_host", leader.getHost(), "leader_port", leader.getPort())));
            } else {
                return new RpcResponse("removeServer",
                    new RpcResponse.RpcError(-32001, "Leader not found", null));
            }
        }
        
        // Check if server exists
        boolean serverExists = clusterMembers.stream()
            .anyMatch(s -> s.getHost().equals(host) && s.getPort() == port);
            
        if (!serverExists) {
            return new RpcResponse("removeServer", 
                new RpcResponse.RpcError(-32002, "Server does not exist in cluster", null));
        }
        
        // Check if configuration change is already in progress
        if (isJointConsensus || jointConfig != null || newConfig != null) {
            return new RpcResponse("removeServer", 
                new RpcResponse.RpcError(-32003, "Configuration change already in progress", null));
        }
        
        // Check if we're removing ourselves (leader)
        boolean removingLeader = host.equals(address.getHostAddress()) && port == this.port;
        
        // Check if we'll have enough servers left
        if (clusterMembers.size() <= 1 || (clusterMembers.size() <= 2 && removingLeader)) {
            return new RpcResponse("removeServer", 
                new RpcResponse.RpcError(-32005, "Cannot remove server: cluster would be too small", null));
        }
        
        // Create new configuration without the removed server
        newConfig = currentConfig.stream()
            .filter(s -> !(s.getHost().equals(host) && s.getPort() == port))
            .collect(java.util.stream.Collectors.toList());
        
        // Start the two-phase configuration change
        startJointConsensus();
        
        return new RpcResponse("removeServer", "Server removal initiated");
    }

    private void startJointConsensus() {
        if (newConfig == null) {
            System.err.println("Cannot start joint consensus: newConfig is null");
            return;
        }
        
        System.out.println("Starting joint consensus phase");
        jointConfig = new ArrayList<>();
        jointConfig.addAll(currentConfig);
        jointConfig.addAll(newConfig.stream()
            .filter(s -> currentConfig.stream()
                .noneMatch(c -> c.getHost().equals(s.getHost()) && c.getPort() == s.getPort()))
            .collect(java.util.stream.Collectors.toList()));

        System.out.println("CURRENT CONFIG: " + currentConfig);
        System.out.println("NEW CONFIG: " + newConfig);
        System.out.println("JOINT CONFIG: " + jointConfig);
        
        // Create configuration change entry for joint consensus (Cold,new)
        ConfigurationChange jointChange = new ConfigurationChange(
            currentConfig.isEmpty() ? null : currentConfig, 
            newConfig, 
            true, 
            currentTerm,
            address.getHostAddress() + ":" + port, 
            lastLogIndex + 1
        );
        
        configChangeIndex = lastLogIndex + 1;
        
        // Send joint config to all servers in both old and new configs
        for (ServerInfo server : jointConfig) {
            NodeConnection connection = nodeConnections.get(server);
            if (connection == null && server.getPort() != this.port) { // Skip self
                connection = new NodeConnection(server);
                nodeConnections.put(server, connection);
                try {
                    connection.connect();
                } catch (IOException e) {
                    System.err.println("Failed to connect to server " + server + ": " + e.getMessage());
                    continue;
                }
            }
            
            if (server.getPort() == this.port) { 
                // Apply joint config locally
                applyJointConsensus(jointChange);
                continue;
            }
            
            if (connection != null && connection.isConnected()) {
                try {
                    connection.send(gson.toJson(jointChange));
                    System.out.println("Sent joint consensus config to " + server.getHost() + ":" + server.getPort());
                } catch (IOException e) {
                    System.err.println("Error sending joint config to " + server + ": " + e.getMessage());
                }
            }
        }
        
        
        configVotes.clear();
        
        // Add self-vote
        String selfId = address.getHostAddress() + ":" + port;
        configVotes.put(selfId, true);
        
        // Start checking for config change completion
        scheduleConfigChangeCheck();
    }

    private void applyJointConsensus(ConfigurationChange config) {
        System.out.println("Applying joint consensus configuration");
        isJointConsensus = true;
        lastLogIndex = config.configIndex;
        
        // Update local info
        if (config.oldConfig != null && !config.oldConfig.isEmpty()) {
            this.currentConfig = new ArrayList<>(config.oldConfig);
        }
        this.newConfig = new ArrayList<>(config.newConfig);

        System.out.println("APPL JOINT CONFIGGGGG " + newConfig);
        System.out.println("APPL CURRENT CONFIGGG: " + currentConfig);
        System.out.println("APPL NEW CONFIGGG: " + newConfig);
        
        // Debug info
        System.out.println("Joint consensus active: using both old and new configurations for decisions");
        System.out.println("Old config servers: " + config.oldConfig.size());
        System.out.println("New config servers: " + config.newConfig.size());
    }

    private void finishConfiguration() {
        if (!isJointConsensus || newConfig == null) {
            System.err.println("Cannot finish configuration: not in joint consensus or newConfig is null");
            return;
        }
        
        System.out.println("Starting final phase of configuration change");
        
        // Create configuration change entry for new config only (Cnew)
        ConfigurationChange newChange = new ConfigurationChange(
            null, newConfig, false, currentTerm,
            address.getHostAddress() + ":" + port, lastLogIndex + 1
        );
        
        configChangeIndex = lastLogIndex + 1;
        
        // Send new config to all servers in new config
        for (ServerInfo server : newConfig) {
            NodeConnection connection = nodeConnections.get(server);
            if (connection == null && server.getPort() != this.port) { // Skip self
                System.err.println("Missing connection for " + server);
                continue;
            }
            
            if (server.getPort() == this.port) { // Self
                // Apply new config locally
                applyNewConfiguration(newChange);
                continue;
            }
            
            if (connection != null && connection.isConnected()) {
                try {
                    connection.send(gson.toJson(newChange));
                    System.out.println("Sent new configuration to " + server);
                } catch (IOException e) {
                    System.err.println("Error sending new config to " + server + ": " + e.getMessage());
                }
            }
        }
        
        
        configVotes.clear();
        
        // Add self-vote
        String selfId = address.getHostAddress() + ":" + port;
        configVotes.put(selfId, true);
        
        // Continue checking for config change completion
        scheduleConfigChangeCheck();
    }

    private void applyNewConfiguration(ConfigurationChange config) {
        System.out.println("Applying new configuration");
        isJointConsensus = false;
        lastLogIndex = config.configIndex;
        
        // Update local info
        this.currentConfig = new ArrayList<>(config.newConfig);
        this.jointConfig = null;
        
        // Update clusterMembers to reflect new configuration
        clusterMembers.clear();
        clusterMembers.addAll(currentConfig);
        
        // Check if we're still in the configuration
        boolean stillInConfig = currentConfig.stream()
            .anyMatch(s -> s.getHost().equals(address.getHostAddress()) && s.getPort() == port);
        
        if (!stillInConfig) {
            System.out.println("This server is no longer in the new configuration. Stepping down.");
            nodeType = NodeType.FOLLOWER;
            // Stop heartbeat if we're stepping down
            if (heartbeatExecutor != null) {
                heartbeatExecutor.shutdown();
            }
        }
        
        
        System.out.println("New configuration active with " + config.newConfig.size() + " servers");
        
        // Clean up connections to servers not in new config
        for (Iterator<Map.Entry<ServerInfo, NodeConnection>> it = nodeConnections.entrySet().iterator(); it.hasNext();) {
            Map.Entry<ServerInfo, NodeConnection> entry = it.next();
            ServerInfo server = entry.getKey();
            
            boolean inNewConfig = currentConfig.stream()
                .anyMatch(s -> s.getHost().equals(server.getHost()) && s.getPort() == server.getPort());
                
            if (!inNewConfig) {
                System.out.println("Closing connection to server no longer in config: " + server);
                entry.getValue().disconnect();
                it.remove();
            }
        }
    }

    private void scheduleConfigChangeCheck() {
        heartbeatExecutor.schedule(() -> {
            // Check if we have majority acceptance for config change
            int requiredVotes;
            
            if (isJointConsensus) {
                // During joint consensus, we need majority from both old and new configs
                int oldConfigSize = currentConfig.size();
                int newConfigSize = newConfig.size();
                int oldMajority = oldConfigSize / 2 + 1;
                int newMajority = newConfigSize / 2 + 1;
                
                // Count votes from old and new configs separately
                long oldVotes = configVotes.entrySet().stream()
                    .filter(Map.Entry::getValue)
                    .filter(e -> {
                        String[] parts = e.getKey().split(":");
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        return currentConfig.stream()
                            .anyMatch(s -> s.getHost().equals(host) && s.getPort() == port);
                    })
                    .count();
                    
                long newVotes = configVotes.entrySet().stream()
                    .filter(Map.Entry::getValue)
                    .filter(e -> {
                        String[] parts = e.getKey().split(":");
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);
                        return newConfig.stream()
                            .anyMatch(s -> s.getHost().equals(host) && s.getPort() == port);
                    })
                    .count();
                    
                System.out.println("Joint consensus votes - Old config: " + oldVotes + "/" + oldMajority + 
                    ", New config: " + newVotes + "/" + newMajority);
                    
                // We need majority in both configs
                if (oldVotes >= oldMajority && newVotes >= newMajority) {
                    System.out.println("Joint consensus achieved! Moving to final configuration phase");
                    finishConfiguration();
                    return;
                }
            } else {
                // For final config, we just need majority of new config
                int newConfigSize = currentConfig.size();
                requiredVotes = newConfigSize / 2 + 1;
                
                long totalVotes = configVotes.values().stream().filter(v -> v).count();
                System.out.println("New configuration votes: " + totalVotes + "/" + requiredVotes);
                
                if (totalVotes >= requiredVotes) {
                    System.out.println("New configuration committed! Configuration change complete");
                    
                    // Configuration change is complete, clean up
                    newConfig = null;
                    configChangeIndex = -1;
                    return;
                }
            }
            
            // If we haven't achieved consensus, retry after a delay
            if (configChangeIndex != -1) {
                scheduleConfigChangeCheck();
            }
        }, 1000, MILLISECONDS);
    }

    // Add these methods to handle configuration change messages
    private void handleConfigurationChange(SocketChannel channel, ConfigurationChange config) {
        if (config.term < currentTerm) {
            // Reject older terms
            sendConfigResponse(channel, false);
            return;
        }
        
        if (config.term > currentTerm) {
            currentTerm = config.term;
            nodeType = NodeType.FOLLOWER;
        }
        
        System.out.println("Received configuration change from leader");
        
        // Update configuration based on message type
        if (config.isJointConsensus) {
            applyJointConsensus(config);
        } else {
            applyNewConfiguration(config);
        }
        
        // Send response to leader
        sendConfigResponse(channel, true);
    }

    private void sendConfigResponse(SocketChannel channel, boolean success) {
        try {
            ConfigChangeResponse response = new ConfigChangeResponse(
                success, 
                currentTerm, 
                address.getHostAddress() + ":" + port
            );
            
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
            
            System.out.println("Sent configuration change response: " + success);
        } catch (IOException e) {
            System.err.println("Error sending config response: " + e.getMessage());
        }
    }

    private void handleConfigurationResponse(ConfigChangeResponse response) {
        if (response.term > currentTerm) {
            currentTerm = response.term;
            nodeType = NodeType.FOLLOWER;
            return;
        }
        
        if (configChangeIndex == -1) {
            // No ongoing config change, ignore
            return;
        }
        
        // Record the vote
        configVotes.put(response.nodeId, response.success);
        System.out.println("Received config change response from " + response.nodeId + ": " + response.success);
    }

    // SAMPE SINIII
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java RaftNode <host> <port> <nodeType>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        NodeType nodeType = NodeType.valueOf(args[2].toUpperCase());

        try {
            RaftNode node = new RaftNode(host, port, nodeType);
            node.startServer();
        } catch (IOException e) {
            System.err.println("Error starting server: " + e.getMessage());
        }
    }
}
