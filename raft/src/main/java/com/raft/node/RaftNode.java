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
import java.util.HashMap;
import java.util.Random;
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

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

    // Constants
    private static final long HEARTBEAT_INTERVAL = 5000; // 2 seconds
    private static final Random random = new Random();

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
        
        initializeClusterMembers();
        for (ServerInfo member : clusterMembers) {
            if (member.getPort() != this.port) {
                this.nodeConnections.put(member, new NodeConnection(member));
            }
            System.out.println("Cluster member: " + member.getHost() + ":" + member.getPort());
        }

        if (nodeType == NodeType.LEADER) {
            System.out.println("Node ini adalah leader");
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
                    } else if (message.contains("\"type\":\"CLUSTER_CONFIG\"")) {
                        System.out.println("Received cluster configuration update");
                        handleClusterConfig(message);
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

            case "add_server":
                if (nodeType != NodeType.LEADER) {
                    return redirectToLeader(message.getId());
                }
                String host = (String) params.get("host");
                int port = ((Number) params.get("port")).intValue();
                boolean success = addServer(host, port);
                return new RpcResponse(message.getId(), success);
                
            case "remove_server":
                if (nodeType != NodeType.LEADER) {
                    return redirectToLeader(message.getId());
                }
                host = (String) params.get("host");
                port = ((Number) params.get("port")).intValue();
                success = removeServer(host, port);
                return new RpcResponse(message.getId(), success);
                
            case "list_servers":
                List<Map<String, Object>> serverList = new ArrayList<>();
                for (ServerInfo info : getClusterMembers()) {
                    Map<String, Object> serverInfo = new HashMap<>();
                    serverInfo.put("host", info.getHost());
                    serverInfo.put("port", info.getPort());
                    serverInfo.put("type", info.getType().toString());
                    serverInfo.put("connected", info.isConnected());
                    serverList.add(serverInfo);
                }
                return new RpcResponse(message.getId(), serverList);
                
            default:
                return new RpcResponse(message.getId(), 
                    new RpcResponse.RpcError(-32601, "Method not found", null));
        }
    }

    private RpcResponse redirectToLeader(String id) {
        // Find current leader
        ServerInfo leader = clusterMembers.stream()
            .filter(s -> s.getType() == NodeType.LEADER)
            .findFirst()
            .orElse(null);

        if (leader != null) {
            return new RpcResponse(id, 
                new RpcResponse.RpcError(-32000, 
                    "Not leader. Please connect to leader.", 
                    Map.of("leader_host", leader.getHost(), 
                        "leader_port", leader.getPort())));
        } else {
            return new RpcResponse(id,
                new RpcResponse.RpcError(-32001, "Leader not found", null));
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

    public boolean addServer(String host, int port) {
        // Only leaders can change membership
        if (nodeType != NodeType.LEADER) {
            return false;
        }
        
        // Check if server already exists in cluster
        for (ServerInfo member : clusterMembers) {
            if (member.getHost().equals(host) && member.getPort() == port) {
                System.out.println("Server " + host + ":" + port + " already exists in cluster");
                return false;
            }
        }   
        
        // Add new server to cluster
        ServerInfo newServer = new ServerInfo(host, port, NodeType.FOLLOWER);
        clusterMembers.add(newServer);
        nodeConnections.put(newServer, new NodeConnection(newServer));
        
        // Try to connect to the new server
        try {
            NodeConnection connection = nodeConnections.get(newServer);
            connection.connect();
            
            // Send the current cluster configuration to the new server
            sendClusterConfig(connection);
            
            System.out.println("Added new server to cluster: " + host + ":" + port);
            return true;
        } catch (IOException e) {
            System.err.println("Error connecting to new server: " + e.getMessage());
            return false;
        }
    }

    public boolean removeServer(String host, int port) {
        // Only leaders can change membership
        if (nodeType != NodeType.LEADER) {
            return false;
        }
        
        // Find server in cluster
        ServerInfo toRemove = null;
        for (ServerInfo member : clusterMembers) {
            if (member.getHost().equals(host) && member.getPort() == port) {
                toRemove = member;
                break;
            }
        }
        
        if (toRemove == null) {
            System.out.println("Server " + host + ":" + port + " not found in cluster");
            return false;
        }
        
        // Cannot remove self
        if (toRemove.getPort() == this.port) {
            System.out.println("Cannot remove self from cluster");
            return false;
        }
        
        // Remove server from cluster
        clusterMembers.remove(toRemove);
        
        // Close connection if it exists
        NodeConnection connection = nodeConnections.remove(toRemove);
        if (connection != null) {
            connection.disconnect();
        }
        
        // Notify all remaining servers about the membership change
        broadcastClusterConfig();
        
        System.out.println("Removed server from cluster: " + host + ":" + port);
        return true;
    }

    private void sendClusterConfig(NodeConnection connection) { 
        if (!connection.isConnected()) {
            return;
        }
        
        try {
            // Create a list of server addresses in the format "host:port"
            List<String> serverList = new ArrayList<>();
            for (ServerInfo server : clusterMembers) {
                serverList.add(server.getHost() + ":" + server.getPort());
            }
            
            // Create and send config message
            String configMessage = String.format(
                "{\"type\":\"CLUSTER_CONFIG\",\"term\":%d,\"servers\":%s}",
                currentTerm,
                gson.toJson(serverList)
            );
            
            connection.send(configMessage);
        } catch (IOException e) {
            System.err.println("Error sending cluster config: " + e.getMessage());
            connection.disconnect();
        }
    }

    private void broadcastClusterConfig() {
        for (Map.Entry<ServerInfo, NodeConnection> entry : nodeConnections.entrySet()) {
            NodeConnection connection = entry.getValue();
            if (connection.isConnected()) {
                sendClusterConfig(connection);
            }
        }
    }

    public List<ServerInfo> getClusterMembers() {
        return new ArrayList<>(clusterMembers);
    }

    private void handleClusterConfig(String message) {
        try {
            JsonObject config = gson.fromJson(message, JsonObject.class);
            JsonArray servers = config.getAsJsonArray("servers");
            
            // Only accept configs from current leader or with higher term
            long configTerm = config.get("term").getAsLong();
            if (configTerm < currentTerm) {
                return;
            }
            
            // Update term if necessary
            if (configTerm > currentTerm) {
                currentTerm = configTerm;
                votedFor = null;
            }
            
            // Clear existing members and add new ones
            List<ServerInfo> newMembers = new ArrayList<>();
            
            // Keep our own entry
            for (ServerInfo self : clusterMembers) {
                if (self.getPort() == this.port) {
                    newMembers.add(self);
                    break;
                }
            }
            
            // Add all servers from config
            for (JsonElement serverElem : servers) {
                String serverAddr = serverElem.getAsString();
                String[] parts = serverAddr.split(":");
                if (parts.length == 2) {
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);
                    
                    // Skip our own entry
                    if (port == this.port) continue;
                    
                    ServerInfo info = new ServerInfo(host, port, NodeType.FOLLOWER);
                    newMembers.add(info);
                    
                    // If this is a new server, create a connection
                    boolean found = false;
                    for (ServerInfo existing : clusterMembers) {
                        if (existing.getPort() == port && existing.getHost().equals(host)) {
                            found = true;
                            break;
                        }
                    }
                    
                    if (!found) {
                        nodeConnections.put(info, new NodeConnection(info));
                    }
                }
            }
            
            // Update cluster members
            clusterMembers.clear();
            clusterMembers.addAll(newMembers);
            
            System.out.println("Updated cluster configuration. New members:");
            for (ServerInfo member : clusterMembers) {
                System.out.println("- " + member);
            }
        } catch (Exception e) {
            System.err.println("Error processing cluster config: " + e.getMessage());
        }
    }
}
