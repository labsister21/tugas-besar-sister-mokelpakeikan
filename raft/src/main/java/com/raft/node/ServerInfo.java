package com.raft.node;

public class ServerInfo {
    private final String host;
    private final int port;
    private final NodeType type;
    private boolean isConnected;

    public ServerInfo(String host, int port, NodeType type) {
        this.host = host;
        this.port = port;
        this.type = type;
        this.isConnected = false;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public NodeType getType() {
        return type;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(boolean connected) {
        isConnected = connected;
    }

    @Override
    public String toString() {
        return String.format("%s:%d (%s)", host, port, type);
    }
} 