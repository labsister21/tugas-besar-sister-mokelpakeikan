package com.rafted;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar rafted-server-jar-with-dependencies.jar <node_id> [<port>]");
            System.exit(1);
        }
        
        int nodeId = Integer.parseInt(args[0]);
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 5000 + nodeId;
        
        // Define all nodes in the cluster
        List<RaftNode.NodeInfo> allNodes = new ArrayList<>();
        allNodes.add(new RaftNode.NodeInfo(1, "localhost", 5001));
        allNodes.add(new RaftNode.NodeInfo(2, "localhost", 5002));
        allNodes.add(new RaftNode.NodeInfo(3, "localhost", 5003));
         allNodes.add(new RaftNode.NodeInfo(4, "localhost", 5004));
        
        // Update the port for the current node in the list
        for (int i = 0; i < allNodes.size(); i++) {
            if (allNodes.get(i).getNodeId() == nodeId) {
                allNodes.set(i, new RaftNode.NodeInfo(nodeId, "localhost", port));
                break;
            }
        }
        
        try {
            // Start the server node
            RaftNode node = new RaftNode(nodeId, "localhost", port, allNodes);
            NetworkManager networkManager = new NetworkManager(node, port);
            
            logger.info("Server node " + nodeId + " started on port " + port);
            
            // Keep the main thread alive
            while (true) {
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            logger.severe("Error starting server node: " + e.getMessage());
            System.exit(1);
        }
    }
} 