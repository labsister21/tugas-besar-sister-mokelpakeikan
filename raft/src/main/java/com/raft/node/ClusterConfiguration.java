package com.raft.node;

import java.util.HashSet;
import java.util.Set;
import java.io.Serializable;

public class ClusterConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final Set<ServerInfo> servers;
    private final long configIndex;
    private final String configId;
    private final ConfigType type;
    
    public enum ConfigType {
        OLD,        // Cold: Original configuration
        JOINT,      // Cold,new: Joint consensus configuration
        NEW         // Cnew: New configuration
    }
    
    public ClusterConfiguration(Set<ServerInfo> servers, long configIndex, ConfigType type) {
        this.servers = new HashSet<>(servers);
        this.configIndex = configIndex;
        this.configId = "config_" + configIndex + "_" + System.currentTimeMillis();
        this.type = type;
    }
    
    public Set<ServerInfo> getServers() {
        return new HashSet<>(servers);
    }
    
    public long getConfigIndex() {
        return configIndex;
    }
    
    public String getConfigId() {
        return configId;
    }
    
    public ConfigType getType() {
        return type;
    }
    
    public boolean containsServer(String host, int port) {
        for (ServerInfo server : servers) {
            if (server.getHost().equals(host) && server.getPort() == port) {
                return true;
            }
        }
        return false;
    }
    
    public ServerInfo getServer(String host, int port) {
        for (ServerInfo server : servers) {
            if (server.getHost().equals(host) && server.getPort() == port) {
                return server;
            }
        }
        return null;
    }
}