package com.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;

import com.raft.rpc.RpcMessage;
import com.raft.rpc.RpcResponse;

public class App {
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private String currentLeaderHost;
    private int currentLeaderPort;
    private boolean connected;
    private int nextId = 1;

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
            connected = true;
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

    private synchronized int getNextId() {
        return nextId++;
    }
    
    private String sendRequest(String requestJson) throws IOException {
        if (clientSocket == null || clientSocket.isClosed()) {
            throw new IOException("Not connected to server");
        }
        
        out.println(requestJson);
        String response = in.readLine();
        
        if (response == null) {
            disconnect();
            throw new IOException("Connection closed by server");
        }
        
        return response;
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

                // Check if auto_redirect flag is set
                boolean autoRedirect = leaderInfo.containsKey("auto_redirect") && 
                                    (boolean) leaderInfo.get("auto_redirect");
                
                // Update leader information
                this.currentLeaderHost = newLeaderHost;
                this.currentLeaderPort = newLeaderPort;
                
                if (autoRedirect) {
                    System.out.println("Not leader. Attempting automatic redirect to " + 
                                    newLeaderHost + ":" + newLeaderPort);
                    
                    // Disconnect from current server
                    disconnect();
                    
                    // Connect to the leader
                    boolean connected = connect();
                    
                    if (connected) {
                        System.out.println("Automatically redirected to leader at " + 
                                        newLeaderHost + ":" + newLeaderPort);
                        // Retry the original request with the new leader
                        return sendRequest(method, params);
                    } else {
                        return new RpcResponse(id, 
                            new RpcResponse.RpcError(-32002, 
                            "Failed to connect to leader at " + 
                            newLeaderHost + ":" + newLeaderPort, null));
                    }
                } else {
                    // No auto-redirect, just return the error
                    return new RpcResponse(id, 
                        new RpcResponse.RpcError(-32000, 
                            String.format("Please reconnect to leader at %s:%d", newLeaderHost, newLeaderPort), 
                            leaderInfo));
                }
            }
            
            return rpcResponse;
        } catch (IOException e) {
            disconnect();
            return new RpcResponse(UUID.randomUUID().toString(), 
                new RpcResponse.RpcError(-32002, "Error communicating with server: " + e.getMessage(), null));
        }
    }
    
    private String parseResponse(String response) {
        try {
            RpcResponse rpcResponse = RpcResponse.fromJson(response);
            
            if (rpcResponse.getError() != null) {
                // Check if we need to redirect to leader
                if (rpcResponse.getError().getCode() == -32000 && rpcResponse.getError().getData() != null) {
                    Map<String, Object> leaderInfo = (Map<String, Object>) rpcResponse.getError().getData();
                    String newLeaderHost = (String) leaderInfo.get("leader_host");
                    int newLeaderPort = ((Number) leaderInfo.get("leader_port")).intValue();
                    
                    // Update leader information
                    this.currentLeaderHost = newLeaderHost;
                    this.currentLeaderPort = newLeaderPort;
                    
                    return "Not leader. Please reconnect to leader at " + newLeaderHost + ":" + newLeaderPort;
                }
                
                return "Error: " + rpcResponse.getError().getMessage();
            }
            
            return rpcResponse.getResult() != null ? rpcResponse.getResult().toString() : "OK";
        } catch (Exception e) {
            return "Error parsing response: " + e.getMessage();
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

    public String getLog() {
        RpcResponse response = sendRequest("getlog", null);
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
            connected = false;
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

    public String addServer(String host, int port) {
        if (!connected) {
            return "Tidak terhubung ke server.";
        }

        try {
            String requestJson = String.format(
                "{\"jsonrpc\": \"2.0\", \"method\": \"addServer\", \"id\": %d, \"params\": {\"host\": \"%s\", \"port\": %d}}",
                getNextId(), host, port
            );
            
            String response = sendRequest(requestJson);
            return parseResponse(response);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }

    public String removeServer(String host, int port) {
        if (!connected) {
            return "Tidak terhubung ke server.";
        }

        try {
            String requestJson = String.format(
                "{\"jsonrpc\": \"2.0\", \"method\": \"removeServer\", \"id\": %d, \"params\": {\"host\": \"%s\", \"port\": %d}}",
                getNextId(), host, port
            );
            
            String response = sendRequest(requestJson);
            return parseResponse(response);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }
}