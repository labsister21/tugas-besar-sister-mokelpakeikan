package com.raft.server;

import com.raft.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RaftServerMain {
    private static final Logger logger = LoggerFactory.getLogger(RaftServerMain.class);
    private static final Map<String, Integer> SERVER_PORTS = new HashMap<>();
    
    static {
        SERVER_PORTS.put("server1", 8001);
        SERVER_PORTS.put("server2", 8002);
        SERVER_PORTS.put("server3", 8003);
        SERVER_PORTS.put("server4", 8004);
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java RaftServerMain <serverId>");
            System.exit(1);
        }

        String serverId = args[0];
        if (!SERVER_PORTS.containsKey(serverId)) {
            System.err.println("Invalid server ID. Must be one of: " + String.join(", ", SERVER_PORTS.keySet()));
            System.exit(1);
        }

        // Create cluster members map
        Map<String, InetSocketAddress> clusterMembers = new HashMap<>();
        for (Map.Entry<String, Integer> entry : SERVER_PORTS.entrySet()) {
            if (!entry.getKey().equals(serverId)) {
                String host = Config.getHostname(entry.getKey());
                clusterMembers.put(entry.getKey(), new InetSocketAddress(host, entry.getValue()));
                logger.info("Added cluster member: {} -> {}:{}", entry.getKey(), host, entry.getValue());
            }
        }

        // Create and start server
        int port = SERVER_PORTS.get(serverId);
        logger.info("Starting Raft server {} on port {} in local mode", serverId, port);
        
        RaftServer server = new RaftServer(serverId, port, clusterMembers);
        server.start();
        
        logger.info("Raft server {} started successfully", serverId);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server {}", serverId);
            server.stop();
        }));
    }
} 