package com.raft.node;

import com.google.gson.Gson;
import com.raft.rpc.HeartbeatMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

public class NodeConnection {
    private final ServerInfo serverInfo;
    private SocketChannel channel;
    private final AtomicBoolean isConnected;
    private long lastHeartbeat;
    private final Gson gson;

    public NodeConnection(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
        this.isConnected = new AtomicBoolean(false);
        this.lastHeartbeat = 0;
        this.gson = new Gson();
    }

    public boolean connect() {
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(serverInfo.getHost(), serverInfo.getPort()));
            
            // Wait for connection to complete
            while (!channel.finishConnect()) {
                Thread.sleep(100);
            }
            
            isConnected.set(true);
            return true;
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed to connect to " + serverInfo + ": " + e.getMessage());
            disconnect();
            return false;
        }
    }

    public void disconnect() {
        isConnected.set(false);
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                System.err.println("Error closing connection to " + serverInfo + ": " + e.getMessage());
            }
        }
    }

    public boolean sendHeartbeat(String leaderId, long term) {
        if (!isConnected.get() || channel == null) {
            return false;
        }

        try {
            HeartbeatMessage heartbeat = new HeartbeatMessage(leaderId, term);
            String message = gson.toJson(heartbeat) + "\n";
            ByteBuffer buffer = ByteBuffer.wrap(message.getBytes());
            
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            
            return true;
        } catch (IOException e) {
            System.err.println("Failed to send heartbeat to " + serverInfo + ": " + e.getMessage());
            disconnect();
            return false;
        }
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public boolean isConnected() {
        return isConnected.get();
    }

    public void updateLastHeartbeat() {
        this.lastHeartbeat = System.currentTimeMillis();
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public ServerInfo getServerInfo() {
        return serverInfo;
    }
} 