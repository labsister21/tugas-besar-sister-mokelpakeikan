package com.raft;

import com.raft.node.NodeType;
import com.raft.node.RaftNode;

public class RaftServer {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java RaftServer <host> <port> <nodeType>");
            System.out.println("nodeType can be: LEADER, FOLLOWER");
            System.exit(1);
        }

        String host = args[0];
        int port;
        try {
            port = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Port must be a number");
            return;
        }

        NodeType nodeType;
        try {
            nodeType = NodeType.valueOf(args[2].toUpperCase());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid node type. Must be either LEADER or FOLLOWER");
            return;
        }

        try {
            RaftNode node = new RaftNode(host, port, nodeType);
            
            // Add shutdown hook to handle graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nShutting down RaftNode...");
                node.stopServer();
            }));

            // Start the server
            node.startServer();
        } catch (Exception e) {
            System.err.println("Error starting RaftNode: " + e.getMessage());
            e.printStackTrace();
        }
    }
} 