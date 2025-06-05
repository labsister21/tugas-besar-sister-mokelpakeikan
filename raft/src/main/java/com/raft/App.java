package com.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.List;  // Add this import
import java.util.ArrayList;  // Add this import

import com.raft.rpc.RpcMessage;
import com.raft.rpc.RpcResponse;

public class App {
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private String currentLeaderHost;
    private int currentLeaderPort;

    public App(String initialHost, int initialPort) {
        this.currentLeaderHost = initialHost;
        this.currentLeaderPort = initialPort;
    }

    public boolean connect() {
        try {
            System.out.printf("Mencoba terhubung ke %s:%d...%n", currentLeaderHost, currentLeaderPort);
            clientSocket = new Socket(currentLeaderHost, currentLeaderPort);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            System.out.println("Terhubung ke server.");
            return true;
        } catch (UnknownHostException e) {
            System.err.println("Host tidak diketahui: " + currentLeaderHost);
            return false;
        } catch (IOException e) {
            System.err.println("Tidak bisa mendapatkan I/O untuk koneksi ke " + currentLeaderHost + ":" + currentLeaderPort);
            return false;
        }
    }

    private RpcResponse sendRequest(String method, Map<String, Object> params) {
        if (clientSocket == null || clientSocket.isClosed()) {
            return new RpcResponse(UUID.randomUUID().toString(), 
                new RpcResponse.RpcError(-32003, "Not connected to server", null));
        }

        try {
            String id = UUID.randomUUID().toString();
            RpcMessage request = new RpcMessage(id, method, params);
            out.println(request.toJson());
            
            String response = in.readLine();
            if (response == null) {
                disconnect();
                return new RpcResponse(id, 
                    new RpcResponse.RpcError(-32002, "Connection closed by server", null));
            }

            RpcResponse rpcResponse = RpcResponse.fromJson(response);
            
            // Check if we need to redirect to leader
            if (rpcResponse.getError() != null && rpcResponse.getError().getCode() == -32000) {
                Map<String, Object> leaderInfo = (Map<String, Object>) rpcResponse.getError().getData();
                String newLeaderHost = (String) leaderInfo.get("leader_host");
                int newLeaderPort = ((Number) leaderInfo.get("leader_port")).intValue();
                
                // Update leader information
                this.currentLeaderHost = newLeaderHost;
                this.currentLeaderPort = newLeaderPort;
                
                return new RpcResponse(id, 
                    new RpcResponse.RpcError(-32000, 
                        String.format("Please reconnect to leader at %s:%d", newLeaderHost, newLeaderPort), 
                        leaderInfo));
            }
            
            return rpcResponse;
        } catch (IOException e) {
            disconnect();
            return new RpcResponse(UUID.randomUUID().toString(), 
                new RpcResponse.RpcError(-32002, "Error communicating with server: " + e.getMessage(), null));
        }
    }

    public String ping() {
        RpcResponse response = sendRequest("ping", null);
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return (String) response.getResult();
    }

    public String get(String key) {
        RpcResponse response = sendRequest("get", Map.of("key", key));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return response.getResult() != null ? response.getResult().toString() : "NIL";
    }

    public String set(String key, String value) {
        RpcResponse response = sendRequest("set", Map.of("key", key, "value", value));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return "OK";
    }

    public String append(String key, String value) {
        RpcResponse response = sendRequest("append", Map.of("key", key, "value", value));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return response.getResult().toString();
    }

    public String del(String key) {
        RpcResponse response = sendRequest("del", Map.of("key", key));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return response.getResult().toString();
    }

    public String strln(String key) {
        RpcResponse response = sendRequest("strln", Map.of("key", key));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return response.getResult().toString();
    }

    public void disconnect() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (clientSocket != null) clientSocket.close();
            System.out.println("Koneksi ditutup.");
        } catch (IOException e) {
            System.err.println("Error saat menutup koneksi: " + e.getMessage());
        }
    }

    public String getCurrentLeaderHost() {
        return currentLeaderHost;
    }

    public int getCurrentLeaderPort() {
        return currentLeaderPort;
    }

    // Add these methods to the App class
    public String addServer(String host, int port) {
        RpcResponse response = sendRequest("add_server", Map.of("host", host, "port", port));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return "Server added: " + response.getResult();
    }

    public String removeServer(String host, int port) {
        RpcResponse response = sendRequest("remove_server", Map.of("host", host, "port", port));
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        return "Server removed: " + response.getResult();
    }

    public String getCluster() {
        RpcResponse response = sendRequest("get_cluster", null);
        if (response.getError() != null) {
            return "Error: " + response.getError().getMessage();
        }
        
        Map<String, Object> configInfo = (Map<String, Object>) response.getResult();
        StringBuilder result = new StringBuilder("Cluster Configuration:\n");
        
        String configType = (String) configInfo.get("current_config_type");
        long configIndex = ((Number) configInfo.get("current_config_index")).longValue();
        
        result.append(String.format("Config Type: %s, Index: %d\n", configType, configIndex));
        result.append("Servers:\n");
        
        List<Map<String, Object>> servers = (List<Map<String, Object>>) configInfo.get("servers");
        for (Map<String, Object> server : servers) {
            String host = (String) server.get("host");
            int serverPort = ((Number) server.get("port")).intValue();
            String type = (String) server.get("type");
            boolean connected = (boolean) server.get("connected");
            boolean isSelf = (boolean) server.get("self");
            boolean isNonVoting = server.containsKey("non_voting") ? (boolean) server.get("non_voting") : false;
            
            result.append(String.format("  %s:%d (%s) - %s%s%s\n", 
                host, 
                serverPort, 
                type, 
                connected ? "connected" : "disconnected",
                isSelf ? " [self]" : "",
                isNonVoting ? " [non-voting]" : ""));
        }
        
        // Show joint config if present
        if (configInfo.containsKey("joint_config_index")) {
            String jointType = (String) configInfo.get("joint_config_type");
            long jointIndex = ((Number) configInfo.get("joint_config_index")).longValue();
            
            result.append(String.format("\nJoint Configuration: %s, Index: %d\n", jointType, jointIndex));
            
            List<Map<String, Object>> jointServers = (List<Map<String, Object>>) configInfo.get("joint_servers");
            if (jointServers != null) {
                result.append("Joint Config Servers:\n");
                for (Map<String, Object> server : jointServers) {
                    String host = (String) server.get("host");
                    int serverPort = ((Number) server.get("port")).intValue();
                    String type = (String) server.get("type");
                    boolean connected = (boolean) server.get("connected");
                    boolean isSelf = (boolean) server.get("self");
                    
                    result.append(String.format("  %s:%d (%s) - %s%s\n", 
                        host, 
                        serverPort, 
                        type, 
                        connected ? "connected" : "disconnected",
                        isSelf ? " [self]" : ""));
                }
            }
        }
        
        return result.toString();
    }
}
