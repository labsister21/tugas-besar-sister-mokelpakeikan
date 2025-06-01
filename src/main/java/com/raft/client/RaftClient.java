package com.raft.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raft.server.MessageType;
import com.raft.server.RaftMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class RaftClient {
    private static final Logger logger = LoggerFactory.getLogger(RaftClient.class);
    private static final int TIMEOUT_MS = 5000;

    private final Map<String, InetSocketAddress> servers;
    private final ObjectMapper objectMapper;
    private String currentLeader;

    public RaftClient(Map<String, InetSocketAddress> servers) {
        this.servers = new ConcurrentHashMap<>(servers);
        this.objectMapper = new ObjectMapper();
        this.currentLeader = null;
    }

    public String get(String key) throws IOException {
        return executeCommand(MessageType.CLIENT_REQUEST, key, null);
    }

    public boolean set(String key, String value) throws IOException {
        return Boolean.parseBoolean(executeCommand(MessageType.CLIENT_REQUEST, key, value));
    }

    public boolean del(String key) throws IOException {
        return Boolean.parseBoolean(executeCommand(MessageType.CLIENT_REQUEST, key, "DELETE"));
    }

    public boolean append(String key, String value) throws IOException {
        return Boolean.parseBoolean(executeCommand(MessageType.CLIENT_REQUEST, key, "APPEND:" + value));
    }

    private String executeCommand(MessageType type, String key, String value) throws IOException {
        long startTime = System.currentTimeMillis();
        IOException lastException = null;

        while (System.currentTimeMillis() - startTime < TIMEOUT_MS) {
            try {
                if (currentLeader == null) {
                    // Try all servers until we find the leader
                    for (Map.Entry<String, InetSocketAddress> server : servers.entrySet()) {
                        try {
                            String response = sendRequest(server.getValue(), type, key, value);
                            if (response != null) {
                                currentLeader = server.getKey();
                                return response;
                            }
                        } catch (IOException e) {
                            lastException = e;
                            continue;
                        }
                    }
                } else {
                    // Try the current leader
                    InetSocketAddress leaderAddress = servers.get(currentLeader);
                    if (leaderAddress != null) {
                        try {
                            return sendRequest(leaderAddress, type, key, value);
                        } catch (IOException e) {
                            lastException = e;
                            currentLeader = null;
                            continue;
                        }
                    }
                }
            } catch (Exception e) {
                lastException = new IOException("Failed to execute command", e);
            }

            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Command execution interrupted", e);
            }
        }

        throw new IOException("Command execution timed out", lastException);
    }

    private String sendRequest(InetSocketAddress address, MessageType type, String key, String value) throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(address);
            channel.configureBlocking(true);

            RaftMessage request = new RaftMessage(type, 0, "client", key, value);
            String jsonRequest = objectMapper.writeValueAsString(request);
            ByteBuffer buffer = ByteBuffer.wrap(jsonRequest.getBytes());
            channel.write(buffer);

            buffer = ByteBuffer.allocate(1024);
            int bytesRead = channel.read(buffer);
            if (bytesRead > 0) {
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                RaftMessage response = objectMapper.readValue(new String(data), RaftMessage.class);
                return response.getValue();
            }
        }
        return null;
    }
} 