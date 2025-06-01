package com.rafted;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.util.Scanner;
import java.util.logging.Logger;

public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private String currentServerAddress;
    private int currentServerPort;
    private boolean connected = false;
    
    public static void main(String[] args) {
        Client client = new Client();
        client.start();
    }
    
    public void start() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Rafted Client - Type 'help' for available commands");
        
        while (true) {
            System.out.print("> ");
            String input = scanner.nextLine().trim();
            
            if (input.isEmpty()) continue;
            
            String[] parts = input.split("\\s+");
            String command = parts[0].toLowerCase();
            
            try {
                switch (command) {
                    case "help":
                        printHelp();
                        break;
                    case "connect":
                        if (parts.length != 3) {
                            System.out.println("Usage: connect <address> <port>");
                            break;
                        }
                        connect(parts[1], Integer.parseInt(parts[2]));
                        break;
                    case "disconnect":
                        disconnect();
                        break;
                    case "exit":
                        return;
                    default:
                        if (!connected) {
                            System.out.println("Not connected to any server. Use 'connect <address> <port>' first.");
                            break;
                        }
                        handleKeyValueCommand(parts);
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
    
    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  connect <address> <port>  - Connect to a server");
        System.out.println("  disconnect               - Disconnect from current server");
        System.out.println("  get <key>                - Get value for key");
        System.out.println("  set <key> <value>        - Set value for key");
        System.out.println("  del <key>                - Delete key");
        System.out.println("  strln <key>              - Get length of value");
        System.out.println("  append <key> <value>     - Append value to key");
        System.out.println("  ping                     - Check connection");
        System.out.println("  disconnect               - Disconnect from current server");
        System.out.println("  help                     - Show this help");
        System.out.println("  exit                     - Exit the program");
    }
    
    private void connect(String address, int port) {
        try {
            // Test connection with a ping
            Socket socket = new Socket(address, port);
            socket.close();
            currentServerAddress = address;
            currentServerPort = port;
            connected = true;
            System.out.println("Connected to server at " + address + ":" + port);
        } catch (Exception e) {
            System.out.println("Failed to connect: " + e.getMessage());
        }
    }
    
    private void disconnect() {
        if (connected) {
            connected = false;
            currentServerAddress = null;
            currentServerPort = 0;
            System.out.println("Disconnected");
        } else {
            System.out.println("Not connected to any server");
        }
    }
    
    private void handleKeyValueCommand(String[] parts) {
        if (parts.length < 1) return;
        
        String command = parts[0].toLowerCase();
        NetworkManager.Command cmd = null;
        
        switch (command) {
            case "get":
                if (parts.length != 2) {
                    System.out.println("Usage: get <key>");
                    return;
                }

                
                cmd = new NetworkManager.Command(NetworkManager.CommandType.GET, parts[1], null);
                
                break;
            case "set":
                if (parts.length != 3) {
                    System.out.println("Usage: set <key> <value>");
                    return;
                }
                cmd = new NetworkManager.Command(NetworkManager.CommandType.SET, parts[1], parts[2]);
                break;
            case "del":
                if (parts.length != 2) {
                    System.out.println("Usage: del <key>");
                    return;
                }
                cmd = new NetworkManager.Command(NetworkManager.CommandType.DELETE, parts[1], null);
                break;
            case "strln":
                if (parts.length != 2) {
                    System.out.println("Usage: strln <key>");
                    return;
                }
                cmd = new NetworkManager.Command(NetworkManager.CommandType.STRLEN, parts[1], null);
                break;
            case "append":
                if (parts.length != 3) {
                    System.out.println("Usage: append <key> <value>");
                    return;
                }
                cmd = new NetworkManager.Command(NetworkManager.CommandType.APPEND, parts[1], parts[2]);
                break;
            case "ping":
                cmd = new NetworkManager.Command(NetworkManager.CommandType.PING, null, null);
                break;
            case "disconnect":
                disconnect();
                return; 
            default:
                System.out.println("Unknown command: " + command);
                return;
        }
        
        try {
            NetworkManager.Message response = sendCommand(cmd);
            handleResponse(response);
        } catch (Exception e) {
            System.out.println("Error executing command: " + e.getMessage());
        }
    }
    
    private NetworkManager.Message sendCommand(NetworkManager.Command command) throws IOException {
        if (!connected) {
            throw new IOException("Not connected to any server");
        }
        
        try (Socket socket = new Socket(currentServerAddress, currentServerPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            
            NetworkManager.Message request = new NetworkManager.Message(NetworkManager.MessageType.CLIENT_COMMAND, command);
            out.println(objectMapper.writeValueAsString(request));
            
            String responseStr = in.readLine();
            if (responseStr == null) {
                throw new IOException("No response from server");
            }
            
            return objectMapper.readValue(responseStr, NetworkManager.Message.class);
        }
    }
    
    private void handleResponse(NetworkManager.Message response) {
        switch (response.getType()) {
            case CLIENT_RESPONSE:
                System.out.println(response.getData());
                break;
            case NOT_LEADER:
                System.out.println(response.getData());
                break;
            case ERROR:
                System.out.println("Error: " + response.getData());
                break;
            default:
                System.out.println("Unexpected response type: " + response.getType());
        }
    }
} 