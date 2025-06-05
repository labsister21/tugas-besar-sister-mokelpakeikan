package com.raft.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.gson.Gson;

public class NodeConnection {
    private final ServerInfo serverInfo;
    private SocketChannel channel;
    private final AtomicBoolean isConnected;
    private long lastHeartbeat;
    private final Gson gson;
    private int nextIndex; // Track the next log entry to send to this follower

    public NodeConnection(ServerInfo serverInfo) {
        this.serverInfo = serverInfo;
        this.isConnected = new AtomicBoolean(false);
        this.lastHeartbeat = 0;
        this.gson = new Gson();
        this.nextIndex = 1; // Initialize to 1 (first log entry)
    }

    public void connect() throws IOException {
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(serverInfo.getHost(), serverInfo.getPort()));
            // Wait for connection to complete
            while (!channel.finishConnect()) {
                Thread.yield();
            }
            
            isConnected.set(true);
            System.out.println("Connected to " + serverInfo.getHost() + ":" + serverInfo.getPort());
        } catch (Exception e) {
            System.err.println("Failed to connect to " + serverInfo.getHost() + ":" + 
                             serverInfo.getPort() + ": " + e.getMessage());
            disconnect();
        }
        // if (isConnected() || (this.channel != null && this.channel.isConnected())) {
        //     return true; 
        // }
        // if (this.channel == null || !this.channel.isOpen()) {
        //     this.channel = SocketChannel.open();
        //     this.channel.configureBlocking(false); 
        // }
        
        // boolean success = this.channel.connect(new InetSocketAddress(serverInfo.getHost(), serverInfo.getPort()));
        
        // if (success) {
        //     isConnected.set(true);
        // } else {
        //     isConnected.set(false); 
        // }
        // return success;      
    }

    public void disconnect() {
        isConnected.set(false);
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                System.err.println("Error closing channel: " + e.getMessage());
            }
        }
    }

    public boolean isConnected() {
        return isConnected.get() && channel != null && channel.isConnected();
    }

    public void send(String message) throws IOException {
        if (!isConnected()) {
            throw new IOException("Not connected");
        }
        ByteBuffer buffer = ByteBuffer.wrap((message + "\n").getBytes());
        channel.write(buffer);
    }

    public boolean sendHeartbeat(String leaderId, long term) {
        if (!isConnected()) {
            return false;
        }

        try {
            String heartbeat = String.format(
                "{\"type\":\"HEARTBEAT\",\"leaderId\":\"%s\",\"term\":%d}",
                leaderId, term
            );
            send(heartbeat);
            return true;
        } catch (Exception e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
            disconnect();
            return false;
        }
    }

    public SocketChannel getChannel() {
        return channel;
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

    public void setConnected(boolean connected) {
        isConnected.set(connected);
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }
} 