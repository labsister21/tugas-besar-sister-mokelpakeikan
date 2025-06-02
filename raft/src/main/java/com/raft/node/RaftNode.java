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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import java.util.Random;

import com.raft.rpc.RpcMessage;
import com.raft.rpc.RpcResponse;
import com.google.gson.Gson;
import com.raft.rpc.HeartbeatMessage;

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
        
        initializeClusterMembers();
        for (ServerInfo member : clusterMembers) {
            System.out.println("Cluster member: " + member.getHost() + ":" + member.getPort());
        }

        if (nodeType == NodeType.LEADER) {
            System.out.println("Node ini adalah leader");
            initializeNodeConnections();
            startHeartbeat();
        } else {
            System.out.printf("Node ini adalah follower dengan timeout %.2f ms%n", heartbeatTimeout);
            startTimeoutChecker();
        }
    }

    private void initializeClusterMembers() {
        clusterMembers.add(new ServerInfo("localhost", 8001, NodeType.LEADER));
        clusterMembers.add(new ServerInfo("localhost", 8002, NodeType.FOLLOWER));
        clusterMembers.add(new ServerInfo("localhost", 8003, NodeType.FOLLOWER));
        clusterMembers.add(new ServerInfo("localhost", 8004, NodeType.FOLLOWER));
    }

    private void initializeNodeConnections() {
        for (ServerInfo member : clusterMembers) {
            // Skip self-connection for leader
            if (member.getPort() != this.port) {
                NodeConnection connection = new NodeConnection(member);
                nodeConnections.put(member, connection);
            }
        }
    }

    private void startTimeoutChecker() {
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            if (now - lastHeartbeatReceived > heartbeatTimeout) {
                System.out.printf("No heartbeat received for %.2f ms. Leader might be down!%n", 
                    (now - lastHeartbeatReceived));
                nodeType = NodeType.CANDIDATE;
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
                    connection.connect();
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
            lastHeartbeatReceived = System.currentTimeMillis();
            
            // Only print on even timestamps
            if (lastHeartbeatReceived % 2 == 0) {
                System.out.println("Received heartbeat from leader: " + heartbeat.getLeaderId() + 
                                 " (term: " + heartbeat.getTerm() + " heartbeat timeout: " + heartbeatTimeout + ")");
            }
            
            // Update term if necessary
            if (heartbeat.getTerm() > currentTerm) {
                currentTerm = heartbeat.getTerm();
            }
        } catch (Exception e) {
            System.err.println("Error processing heartbeat: " + e.getMessage());
        }
    }

    public void startServer() throws IOException {
        selector = Selector.open();
        serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress(address, port));
        serverSocket.configureBlocking(false);
        serverSocket.register(selector, SelectionKey.OP_ACCEPT);
        
        running = true;
        System.out.printf("RaftNode (%s) berjalan di %s:%d%n", nodeType, address.getHostAddress(), port);

        while (running) {
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
                    }
                }
            } catch (IOException e) {
                System.err.println("Error dalam event loop: " + e.getMessage());
            }
        }
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
        
        try {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                closeConnection(channel);
                return;
            }

            if (bytesRead > 0) {
                buffer.flip();
                byte[] data = new byte[buffer.limit()];
                buffer.get(data);
                buffer.clear();

                String message = new String(data).trim();
                System.out.println("Message received: " + message);

                // Check if it's a heartbeat message
                if (message.contains("\"type\":\"HEARTBEAT\"")) {
                    handleHeartbeat(message);
                } else {
                    processClientMessage(channel, message);
                }
            }
        } catch (IOException e) {
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
