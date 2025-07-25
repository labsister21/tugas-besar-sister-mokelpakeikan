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
import com.sun.tools.jconsole.JConsoleContext;
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
    private Map<NodeConnection, Long> nextIndexMap = null;
    private Map<NodeConnection, Long> matchIndexMap = new ConcurrentHashMap<>();


    // Connections
    private final InetAddress address;
    private final int port;
    private ServerSocketChannel serverSocket;
    private final Map<SocketChannel, ByteBuffer> clientBuffers;
    private final Map<ServerInfo, NodeConnection> nodeConnections;


    // Timeouts / Intervals
    private final int VOTE_TIMEOUT = 5000; // 5 seconds timeout for votes
    private final float heartbeatTimeout;
    private static final long HEARTBEAT_INTERVAL = 10000; // 2 seconds


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
    private boolean heartbeatActive = false;
    private final Map<String, Boolean> configVotes = new ConcurrentHashMap<>();

    // Constants
    private static final Random random = new Random();
    private final Gson gson;
    private final Object voteLock = new Object();
    private final Object logLock = new Object();
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
        this.nextIndexMap = new ConcurrentHashMap<>();
        this.matchIndexMap = new ConcurrentHashMap<>();
        this.gson = new Gson();

        this.currentConfig = new ArrayList<>(clusterMembers);
        this.jointConfig = null;
        this.newConfig = null;
        
        // Load persistent state
        State state = PersistentState.loadState();
        this.currentTerm = state.getCurrentTerm();
        this.votedFor = state.getVotedFor();
        this.logEntries.addAll(state.getLog());
        
        this.heartbeatTimeout = 10000 + random.nextFloat() * 5000;
        this.lastHeartbeatReceived = System.currentTimeMillis();
        
        this.receivedVotes = 0;
        this.lastLogIndex = logEntries.isEmpty() ? 0 : logEntries.size();
        this.lastLogTerm = logEntries.isEmpty() ? 0 : logEntries.get(logEntries.size() - 1).getTerm();
        
        System.out.println("Initialized with lastLogIndex: " + lastLogIndex + ", lastLogTerm: " + lastLogTerm + 
                          ", logEntries.size: " + logEntries.size());
        
        initializeClusterMembers();

        this.currentConfig = new ArrayList<>(clusterMembers);
        this.jointConfig = null;
        this.newConfig = null;

        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) {
                NodeConnection newConnection = new NodeConnection(member);
                this.nodeConnections.put(member, newConnection);
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
            if (nodeType != NodeType.FOLLOWER) {
                return;  // Leader tidak perlu mengecek timeout
            }


            
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatReceived > heartbeatTimeout && !inElection) {
                System.out.println("No heartbeat received as a "+ nodeType + "for " + (now - lastHeartbeatReceived) + " ms. Starting election!");
                lastHeartbeatReceived = now;
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
                    // String leaderId = address.getHostAddress() + ":" + port;
                    // if (!connection.sendHeartbeat(leaderId, currentTerm)) {
                    //     System.err.println("Failed to send heartbeat to " + entry.getKey());
                    // }
                    
                    // Also send AppendEntries heartbeat for log synchronization
                    sendHeartbeatAppendEntries(connection);
                }
            }
        }, 0, HEARTBEAT_INTERVAL, MILLISECONDS);
    }

    private void handleHeartbeat(String message) {
        try {
            HeartbeatMessage heartbeat = gson.fromJson(message, HeartbeatMessage.class);

            // Update leader information in clusterMembers using syncLeaderStatus
            syncLeaderStatus(heartbeat.getLeaderId());
            
            System.out.println("DEBUG: Heartbeat received from " + heartbeat.getLeaderId());
            lastHeartbeatReceived = System.currentTimeMillis();
            
            // Update term if necessary
            if (heartbeat.getTerm() > currentTerm) {
                currentTerm = heartbeat.getTerm();
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                if (nodeType == NodeType.CANDIDATE) {
                    cancelCandidation();
                } else if (nodeType == NodeType.LEADER) {
                    revertToFollower();
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
                System.out.println("votedfor becomes null in cancelcandidation");
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
                // System.out.println("Registered new channel for reading vote responses");
            } else {
                key.interestOps(SelectionKey.OP_READ);
                // System.out.println("Updated existing channel for reading vote responses");
            }
            selector.wakeup();
        } catch (Exception e) {
            System.err.println("Error sending vote request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleVoteRequest(SocketChannel channel, VoteMessage.VoteRequest request) {
        boolean voteGranted = false;
        System.out.println("recieved vote request with current term : " + currentTerm + " and voted for : " + votedFor);
        synchronized (voteLock) {
            if (request.getTerm() > currentTerm) {
                currentTerm = request.getTerm();
                revertToFollower();
                
                lastHeartbeatReceived = System.currentTimeMillis();
                PersistentState.saveState(currentTerm, votedFor, logEntries);
            }
            
            boolean logUpToDate = (request.getLastLogTerm() > lastLogTerm || (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= lastLogIndex));
            if (request.getTerm() == currentTerm && (votedFor == null || votedFor.equals(request.getCandidateId())) && logUpToDate) {
                revertToFollower();
                votedFor = request.getCandidateId();
                PersistentState.saveState(currentTerm, votedFor, logEntries);
                voteGranted = true;
                lastHeartbeatReceived = System.currentTimeMillis();
            }
        }
        System.out.println("voted for : " + votedFor);

        VoteMessage.VoteResponse response = new VoteMessage.VoteResponse(
            currentTerm,
            voteGranted,
            address.getHostAddress() + ":" + port
        );

        long now = System.currentTimeMillis();
        // System.out.println("created vote response for " + request.getCandidateId() + " response : " + voteGranted + " at " + now/1000 + " s with response type : " + response.getType());

        try {
            String jsonResponse = gson.toJson(response);
            ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
            channel.write(buffer);
            // System.out.println("Write Buffer to " + channel.getRemoteAddress() + " done");

        } catch (IOException e) {
            System.err.println("Error sending vote response: " + e.getMessage());
        }
    }

    private void handleVoteResponse(VoteMessage.VoteResponse response) {
        if (nodeType != NodeType.CANDIDATE) {
            System.out.println("Ignoring vote - no longer a candidate");
            return;
        }

        // System.out.println("=== Vote Response Processing ===");
        // System.out.println("Current node type: " + nodeType);
        // System.out.println("Response from: " + response.getVoterId());
        // System.out.println("Vote granted: " + response.isVoteGranted());
        // System.out.println("Current votes: " + receivedVotes);
        // System.out.println("handling vote response from " + response.getVoterId() + " with term " + response.getTerm());

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

            // Update all serverInfo entries - find self by port match
            String leaderId = this.address.getHostAddress() + ":" + this.port;
            syncLeaderStatus(leaderId);

            // Initialize leader state
            this.nextIndexMap.clear();
            this.matchIndexMap.clear();
            long leaderLastLogIndex = lastLogIndex; // Use the actual lastLogIndex
            
            // Initialize nextIndex for all connections to other nodes
            for (NodeConnection connection : nodeConnections.values()) {
                this.matchIndexMap.put(connection, 0L); // Initialize matchIndex to 0
                this.nextIndexMap.put(connection, leaderLastLogIndex + 1);
                // System.out.println("Initialized nextIndex for " + connection.getServerInfo() + " to " + (leaderLastLogIndex + 1));
            }
            
            // System.out.println("Initializing leader state with lastLogIndex: " + leaderLastLogIndex);
            
            startHeartbeat();
        }
    }

    private void revertToFollower() {
        synchronized (voteLock) {
            nodeType = NodeType.FOLLOWER;
            votedFor = null;
            receivedVotes = 0;
            // Update own status in clusterMembers
            for (ServerInfo member : clusterMembers) {
                if (member.getPort() == this.port) {
                    System.out.println("setting member type of " + member.getHost() + ":" + member.getPort() + " to follower");
                    member.setType(NodeType.FOLLOWER);
                    break;
                }
            }

            System.out.println("votedfor becomes null in reverttofollower");
        }

    }

    public void startServer() throws IOException {
        lastHeartbeatReceived = System.currentTimeMillis();
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
            // System.out.println("Read " + bytesRead + " bytes from " + channel.getRemoteAddress());
            if (bytesRead == -1) {
                // System.out.println("bytes read : " + bytesRead + " connection closed from " + channel.getRemoteAddress());
                // System.out.println("--------------------------------");
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
                                // System.out.println("Received log replication confirmation from " + followerId + 
                                //     " for command " + commandId + 
                                //     " (total replicated: " + vote.replicatedNodes.size() + ")");
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

                    // Untuk debug leader info
                    System.out.println("DEBUG: Looking for leader, found: " + (leader != null ? 
                        (leader.getHost() + ":" + leader.getPort() + " type=" + leader.getType()) : "null"));

                    
                    // System.out.println("DEBUG: Current cluster members:");
                    for (ServerInfo member : clusterMembers) {
                        // System.out.println("  - " + member.getHost() + ":" + member.getPort() + " (" + member.getType() + ")");
                    }

                    if (leader != null) {
                        response = new RpcResponse(rpcMessage.getId(), 
                            new RpcResponse.RpcError(-32000, 
                                "Not leader. Please reconnect to leader.", 
                                Map.of("leader_host", leader.getHost(), 
                                    "leader_port", leader.getPort(),
                                    "auto_redirect", true)));
                        
                        // Send response
                        sendResponse(clientChannel, response);
                        
                        // After sending redirect response, close the connection
                        // System.out.println("Closing client connection and redirecting to leader: " + 
                        //                 leader.getHost() + ":" + leader.getPort());
                        closeConnection(clientChannel);
                        return;
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
        CommandVote vote = new CommandVote(message, lastLogIndex + 1);
        commandVotes.put(message.getId(), vote);
        System.out.println("Starting vote process for command ID: " + message.getId() + 
            " with next log index: " + (lastLogIndex + 1) + " (current lastLogIndex: " + lastLogIndex + ")");

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
                    // System.out.println("Active nodes: " + activeNodes + 
                    //     ", Current votes for command " + message.getId() + ": " + vote.votes.size());

                    if (vote.votes.size() >= (activeNodes / 2)) {
                        // We have majority, execute the command
                        // System.out.println("Received majority votes for command " + message.getId() + 
                        //     ", executing command");
                        vote.executed = true;
                        
                        // Calculate the correct log index for the new entry
                        long newLogIndex = lastLogIndex + 1;
                        
                        // Add new entry with the correct index
                        LogEntry newEntry = new LogEntry(
                            message.getMethod(),
                            message.getId(),
                            newLogIndex,  // Use the calculated newLogIndex
                            currentTerm,
                            (Map<String, Object>) message.getParams()
                        );
                        logEntries.add(newEntry);
                        
                        // Update our log index
                        lastLogIndex = newLogIndex;
                        lastLogTerm = currentTerm;
                        
                        // System.out.println("Added new log entry: index=" + newEntry.getLogIndex() + 
                        //     ", term=" + newEntry.getTerm() + ", method=" + newEntry.getMethod());
                        // System.out.println("Updated lastLogIndex to: " + lastLogIndex);
                        
                        // Execute the command
                        RpcResponse rpcResponse = executeCommand(message);
                        
                        // Save state after adding new log entry
                        PersistentState.saveState(currentTerm, votedFor, logEntries);
                        
                        // Wait for all followers to replicate
                        System.out.println("Waiting for log replication from all followers for command " + message.getId());
                        startTime = System.currentTimeMillis();
                        while (System.currentTimeMillis() - startTime < VOTE_TIMEOUT) {
                            if (vote.replicatedNodes.size() >= activeNodes - 1) { // All followers have replicated
                                System.out.println("All followers have replicated command " + message.getId());
                                vote.committed = true;
                                return rpcResponse;
                            }
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                        
                        // If we got here, some followers didn't replicate in time
                        // System.out.println("Warning: Not all followers replicated command " + message.getId() + 
                        //     " (replicated by " + vote.replicatedNodes.size() + " of " + (activeNodes - 1) + " followers)");
                        return rpcResponse;
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
                // System.out.println("Retrying command " + message.getId() + 
                //     " (attempt " + (retryCount + 1) + " of " + MAX_RETRIES + ")");
                // Clear previous votes for this retry
                vote.votes.clear();
                vote.replicatedNodes.clear();
            }
        }

        // If we've exhausted all retries, return timeout error
        // System.out.println("Command " + message.getId() + " timed out after " + MAX_RETRIES + 
        //     " retries. Received " + vote.votes.size() + " votes");
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
                if (nodeType != NodeType.LEADER) {
                    System.out.println("Not leader, cannot send AppendEntries");
                    return;
                }
            
                long followerNextIndex = nextIndexMap.get(connection);
                System.out.println("followerNextIndex dalam sendAppendEntries untuk connection: " + connection.getServerInfo() + " adalah: " + followerNextIndex);
                long prevLogIndex = followerNextIndex - 1;
                long prevLogTerm = 0;

                // System.out.println("=== Preparing AppendEntries ===");
                // System.out.println("Current term: " + currentTerm);
                // System.out.println("Follower nextIndex: " + followerNextIndex);
                // System.out.println("Calculated prevLogIndex: " + prevLogIndex);
                // System.out.println("Log entries size: " + logEntries.size());
                // System.out.println("Last log index: " + lastLogIndex);

                if (prevLogIndex > 0 && prevLogIndex <= logEntries.size()) {
                    LogEntry prevEntry = logEntries.get((int)prevLogIndex - 1);
                    prevLogTerm = prevEntry.getTerm();
                    System.out.println("Found log entry at index " + prevLogIndex + 
                        " with term: " + prevLogTerm + 
                        " (entry: " + prevEntry.getMethod() + ", commandId: " + prevEntry.getCommandId() + ")");
                } else if (prevLogIndex == 0) {
                    System.out.println("Starting from beginning (prevLogIndex=0), using prevLogTerm=0");
                    prevLogTerm = 0;
                } else {
                    System.out.println("No log entry found at index " + prevLogIndex + 
                        ", using prevLogTerm = 0");
                    // If prevLogIndex is beyond our log, we need to adjust
                    if (prevLogIndex > logEntries.size()) {
                        System.out.println("WARNING: prevLogIndex " + prevLogIndex + 
                            " > logEntries.size " + logEntries.size() + 
                            ", adjusting to start from beginning");
                        prevLogIndex = 0;
                        prevLogTerm = 0;
                        nextIndexMap.put(connection, (long) 1);
                    }
                }

                List<LogEntry> newEntries = new ArrayList<>();
                if (command != null) {
                    newEntries.add(new LogEntry(
                        command.getMethod(),
                        command.getId(),
                        prevLogIndex + 1,
                        currentTerm,
                        (Map<String, Object>) command.getParams()
                    ));
                }

                // 4. Buat pesan AppendEntries dengan nilai yang benar
                AppendEntriesMessage appendEntries = new AppendEntriesMessage(
                    address.getHostAddress() + ":" + port,
                    currentTerm,
                    command, // atau 'newEntries' jika Anda memodifikasi kelas pesan
                    prevLogIndex,  // <- Nilai yang benar
                    prevLogTerm    // <- Nilai yang benar
                );
                // =======================================================

                String jsonMessage = gson.toJson(appendEntries);
                System.out.println("Sending AppendEntries to " + connection.getServerInfo() +
                        " for command ID: " + (command != null ? command.getId() : "heartbeat") +
                        " with prevLogIndex: " + prevLogIndex + " and prevLogTerm: " + prevLogTerm +
                        " (current term: " + currentTerm + ")");
                // System.out.println("=== End Preparing AppendEntries ===");
                connection.send(jsonMessage);

            } catch (IOException e) {
                System.err.println("Error sending AppendEntries: " + e.getMessage());
            }
        }

    private void handleAppendEntries(SocketChannel channel, AppendEntriesMessage message) {
        synchronized (voteLock) {
            boolean success = false;
            long matchIndex = 0;

            // Update Leader status
            syncLeaderStatus(message.getLeaderId());
            
            // System.out.println("=== Processing AppendEntries ===");
            // System.out.println("Node: " + address.getHostAddress() + ":" + port);
            // System.out.println("Message term: " + message.getTerm() + ", Current term: " + currentTerm);
            // System.out.println("PrevLogIndex: " + message.getPrevLogIndex() + ", PrevLogTerm: " + message.getPrevLogTerm());
            // System.out.println("LastLogIndex: " + lastLogIndex + ", LogEntries size: " + logEntries.size());
            System.out.println("message term: " + message.getTerm() + " current term: " + currentTerm + " message prevLogIndex: " + message.getPrevLogIndex() + " lastLogIndex: " + lastLogIndex + " message prevLogTerm: " + message.getPrevLogTerm() + " lastLogTerm: " + lastLogTerm);

            // 1. Reply false if term < currentTerm (§5.1)
            if (message.getTerm() < currentTerm) {
                System.out.println("Rejecting: message term < current term");
                success = false;
            } else {
                lastHeartbeatReceived = System.currentTimeMillis();

                // Update term if message term is higher
                if (message.getTerm() > currentTerm) {
                    currentTerm = message.getTerm();
                    PersistentState.saveState(currentTerm, votedFor, logEntries);
                    revertToFollower();
                }
                
                if (message.getPrevLogIndex() > lastLogIndex) {
                    System.out.println("Rejecting: prevLogIndex > lastLogIndex");
                    success = false;
                } else{
                    if (message.getPrevLogIndex() == lastLogIndex && message.getPrevLogTerm() == lastLogTerm) {
                        success = true;
                    }else{
                        // implementasi jika log lebih maju
                        System.out.println("Rejecting: else");
                        success = false;
                    }
                }
                
                if (success && message.getCommand() != null) {
                    System.out.println("Processing command: " + message.getCommand().getId());
                    
                    // Calculate the correct log index for the new entry
                    long newLogIndex = lastLogIndex + 1;
                    matchIndex = newLogIndex;

                    // Add new entry with the correct index
                    LogEntry newEntry = new LogEntry(
                        message.getCommand().getMethod(),
                        message.getCommand().getId(),
                        newLogIndex,  // Use the calculated newLogIndex
                        currentTerm,
                        (Map<String, Object>) message.getCommand().getParams()
                    );
                    logEntries.add(newEntry);
                    
                    // Update our log index
                    lastLogIndex = newLogIndex;
                    lastLogTerm = currentTerm;
                    
                    // System.out.println("Added new log entry: index=" + newEntry.getLogIndex() + 
                    //     ", term=" + newEntry.getTerm() + ", method=" + newEntry.getMethod());
                    // System.out.println("Updated lastLogIndex to: " + lastLogIndex);
                    
                    // Execute the command
                    RpcResponse rpcResponse = executeCommand(message.getCommand());
                    
                    // Save state after adding new log entry
                    PersistentState.saveState(currentTerm, votedFor, logEntries);
                } else if (success && message.getCommand() == null) {
                    // This is a heartbeat AppendEntries - just update lastHeartbeatReceived
                    matchIndex = lastLogIndex; // Use lastLogIndex as matchIndex for heartbeat
                    // System.out.println("Received heartbeat AppendEntries, updated lastHeartbeatReceived");
                }
            }

            // System.out.println("AppendEntries result: " + success);
            // System.out.println("=== End Processing AppendEntries ===");

            // Send response
            AppendEntriesResponse response = new AppendEntriesResponse(
                currentTerm,
                success,
                address.getHostAddress() + ":" + port,
                matchIndex
            );

            try {
                String jsonResponse = gson.toJson(response);
                ByteBuffer buffer = ByteBuffer.wrap((jsonResponse + "\n").getBytes());
                channel.write(buffer);
                // System.out.println("Sent AppendEntries response: " + jsonResponse + 
                //     " for command ID: " + (message.getCommand() != null ? message.getCommand().getId() : "null"));
            } catch (IOException e) {
                System.err.println("Error sending AppendEntries response: " + e.getMessage());
            }
        }
    }

    private void handleAppendEntriesResponse(SocketChannel channel, AppendEntriesResponse response) {
        // System.out.println("Processing AppendEntries response from " + response.getFollowerId());
        if (nodeType != NodeType.LEADER) {
            System.out.println("Ignoring AppendEntries response, not a leader");
            return;
        }
        if (response.getTerm() > currentTerm) {
            currentTerm = response.getTerm();
            PersistentState.saveState(currentTerm, votedFor, logEntries);
            revertToFollower();
            return;
        }

        if (response.isSuccess()) {
            // Find the command vote by follower ID
            for (Map.Entry<String, CommandVote> entry : commandVotes.entrySet()) {
                CommandVote vote = entry.getValue();
                if (!vote.executed) {
                    vote.votes.add(response.getFollowerId());
                    // System.out.println("Added vote from " + response.getFollowerId() + " for command ID: " + entry.getKey());
                    System.out.println("Current votes for command " + entry.getKey() + ": " + vote.votes.size());
                    break;
                }
            }
            
            // Update nextIndex for successful AppendEntries
            ServerInfo followerInfo = null;
            String followerId = response.getFollowerId();
            for (ServerInfo info : clusterMembers) {
                // System.out.println("Checking cluster member: " + info.getPort() + " against response: " + followerId);
                if (followerId.contains(Integer.toString(info.getPort()))) {
                    followerInfo = info;
                    // System.out.println("Found matching follower info: " + followerInfo);
                    break;
                }
            }
            
            if (followerInfo != null) {
                NodeConnection connection = nodeConnections.get(followerInfo);
                // System.out.println("Looking for connection for follower: " + followerInfo);
                // System.out.println("Available connections: " + nodeConnections.keySet());
                
                if (connection != null) {
                    // System.out.println("Found connection for follower: " + followerInfo);
                    
                    matchIndexMap.put(connection, response.getMatchIndex());
                    nextIndexMap.put(connection, response.getMatchIndex() + 1);

                    System.out.println("nextIndex untuk connection: " + connection.getServerInfo() + " setelah successful AppendEntries: " + nextIndexMap.get(connection));
                    
                } else {
                    System.err.println("No connection found for follower: " + response.getFollowerId());
                    System.err.println("Available connections: " + nodeConnections.keySet());
                }
            } else {
                System.err.println("No follower info found for: " + response.getFollowerId());
                System.err.println("Available cluster members: " + clusterMembers);
            }
        } else {
            // Decrement nextIndex and retry with more conservative approach
            ServerInfo followerInfo = null;
            for (ServerInfo info : clusterMembers) {
                // System.out.println("Checking cluster member: " + info.getHost() + ":" + info.getPort() + " against response: " + response.getFollowerId());
                if ((response.getFollowerId().contains(Integer.toString(info.getPort())))) {
                    followerInfo = info;
                    System.out.println("Found matching follower info: " + followerInfo);
                    break;
                }
            }
            
            if (followerInfo != null) {
                NodeConnection connection = nodeConnections.get(followerInfo);
                if (connection != null) {
                    // sendOldLogs(connection);
                    nextIndexMap.put(connection, nextIndexMap.get(connection) - 1);
                }
            }
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

    // utk verif klo mayoritas server di konfig udh setuju perubahan konfigurasi
    // bersihin status perubahan konfig
    // proses perubahan konfigurasi selesai
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

    // Add method to send heartbeat AppendEntries for log synchronization
    private void sendHeartbeatAppendEntries(NodeConnection connection) {
        try {

            if (nodeType != NodeType.LEADER) {
                System.out.println("Not a leader, cannot send heartbeat AppendEntries");
                return;
            }
            long followerNextIndex = nextIndexMap.get(connection);

            System.out.println("followerNextIndex dalam sendHeartbeatAppendEntries untuk connection: " + connection.getServerInfo() + " adalah: " + followerNextIndex);

            RpcMessage command = null;
            LogEntry logEntry = null;
            LogEntry prevEntry = null;
            if (followerNextIndex < lastLogIndex){
                logEntry = logEntries.get((int) followerNextIndex);
                prevEntry = logEntries.get((int) followerNextIndex - 1);
                command = new RpcMessage(logEntry.getCommandId(), logEntry.getMethod(), logEntry.getParams());
            }
            // Send empty AppendEntries (heartbeat) to help with log synchronization
            AppendEntriesMessage appendEntries = new AppendEntriesMessage(
                address.getHostAddress() + ":" + port,
                logEntry != null ? logEntry.getTerm() : currentTerm,
                command, // No command for heartbeat
                prevEntry != null ? prevEntry.getLogIndex() : lastLogIndex,
                prevEntry != null ? prevEntry.getTerm() : lastLogTerm
            );

            String jsonMessage = gson.toJson(appendEntries);
            connection.send(jsonMessage);
            // System.out.println("Sent heartbeat/AppendEntries to " + connection.getServerInfo() +
            //     " with prevLogIndex: " + prevLogIndex + " and prevLogTerm: " + prevLogTerm);
            // System.out.println("=== End Preparing Heartbeat AppendEntries ===");

        } catch (IOException e) {
            System.err.println("Error sending heartbeat AppendEntries: " + e.getMessage());
        }
    }

    // Add method to print current log state for debugging
    private void printCurrentLogState() {
        System.out.println("\n=== Current Log State ===");
        System.out.println("Node: " + address.getHostAddress() + ":" + port);
        System.out.println("Current Term: " + currentTerm);
        System.out.println("Last Log Index: " + lastLogIndex);
        System.out.println("Last Log Term: " + lastLogTerm);
        System.out.println("Log Entries Count: " + logEntries.size());
        System.out.println("Node Type: " + nodeType);
        
        if (!logEntries.isEmpty()) {
            System.out.println("Recent Log Entries:");
            int startIndex = Math.max(0, logEntries.size() - 5); // Show last 5 entries
            for (int i = startIndex; i < logEntries.size(); i++) {
                LogEntry entry = logEntries.get(i);
                System.out.println("  [" + (i + 1) + "] Term: " + entry.getTerm() + 
                    ", Method: " + entry.getMethod() + ", CommandId: " + entry.getCommandId());
            }
        }
        System.out.println("=======================\n");
    }

    // Method to synchronize leader status
    private void syncLeaderStatus(String leaderId) {
        try {
            // Parse the leaderId to get host and port
            String[] parts = leaderId.split(":");
            String leaderHost = parts[0];
            int leaderPort = Integer.parseInt(parts[1]);
            
            String myId = address.getHostAddress() + ":" + port;
            // System.out.println("DEBUG: syncLeaderStatus called with leaderId: " + leaderId + ", my ID is: " + myId);
            
            boolean leaderFound = false;
            boolean iAmTheLeader = port == leaderPort;
            
            // Reset leader status for all nodes - identify leader by PORT only
            for (ServerInfo server : clusterMembers) {
                if (server.getPort() == leaderPort) {
                    // This is the leader, regardless of hostname/IP
                    server.setType(NodeType.LEADER);
                    leaderFound = true;
                    // System.out.println("DEBUG: Marked server " + server.getHost() + ":" + server.getPort() + " as LEADER");
                } else if (server.getType() == NodeType.LEADER) {
                    server.setType(NodeType.FOLLOWER);
                    // System.out.println("DEBUG: Reverted " + server.getHost() + ":" + server.getPort() + " from LEADER to FOLLOWER");
                }
            }
            
            // Update this node's status based on whether it's the leader
            if (iAmTheLeader) {
                if (nodeType != NodeType.LEADER) {
                    // System.out.println("DEBUG: This node is the leader but nodeType is " + nodeType + ", updating to LEADER");
                    nodeType = NodeType.LEADER;
                    // Only the actual leader should initiate heartbeats
                    if (!heartbeatActive) {
                        startHeartbeat();
                    }
                }
            } else {
                if (nodeType == NodeType.LEADER) {
                    // System.out.println("DEBUG: This node is not the leader but nodeType is LEADER, updating to FOLLOWER");
                    nodeType = NodeType.FOLLOWER;
                    // Stop sending heartbeats if we're not the leader
                    stopHeartbeat();
                }
            }
            
            // System.out.println("DEBUG: After syncLeaderStatus, leaderFound=" + leaderFound + ", iAmTheLeader=" + iAmTheLeader);
            
            // Print current cluster state after update
            // System.out.println("DEBUG: Current cluster members after update:");
            for (ServerInfo member : clusterMembers) {
                System.out.println("[DEBUG] in syncleaderstatus:  - " + member.getHost() + ":" + member.getPort() + " (" + member.getType() + ")");
            }
        } catch (Exception e) {
            System.err.println("Error in syncLeaderStatus: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void stopHeartbeat() {
        heartbeatActive = false;
        // You might need additional logic to actually stop the heartbeat
        System.out.println("Stopped sending heartbeats as this node is no longer the leader");
    }
}
