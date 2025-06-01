package com.raft.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.raft.common.RaftMessage;
import com.raft.common.RaftMessage.MessageType;
import com.raft.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RaftCLI {
    private static final Logger logger = LoggerFactory.getLogger(RaftCLI.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, InetSocketAddress> servers;
    private String currentLeader;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static final Map<String, Integer> SERVER_PORTS = new HashMap<>();
    
    static {
        SERVER_PORTS.put("server1", 8001);
        SERVER_PORTS.put("server2", 8002);
        SERVER_PORTS.put("server3", 8003);
        SERVER_PORTS.put("server4", 8004);
    }

    public RaftCLI() {
        // Server configuration based on deployment mode
        servers = new HashMap<>();
        servers.put("server1", new InetSocketAddress(Config.getHostname("server1"), 8001));
        servers.put("server2", new InetSocketAddress(Config.getHostname("server2"), 8002));
        servers.put("server3", new InetSocketAddress(Config.getHostname("server3"), 8003));
        servers.put("server4", new InetSocketAddress(Config.getHostname("server4"), 8004));
        
        if (Config.isDockerMode()) {
            logger.info("Running in Docker mode");
        } else {
            logger.info("Running in local mode");
        }
    }

    public void start() {
        System.out.println("Raft Key-Value Store CLI");
        System.out.println("Available commands:");
        System.out.println("  get <key>");
        System.out.println("  set <key> <value>");
        System.out.println("  del <key>");
        System.out.println("  append <key> <value>");
        System.out.println("  strln <key>");
        System.out.println("  ping");
        System.out.println("  exit");

        // Start leader discovery
        scheduler.scheduleAtFixedRate(this::discoverLeader, 0, 5, TimeUnit.SECONDS);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equalsIgnoreCase("exit")) {
                    break;
                }
                handleCommand(line);
            }
        } catch (IOException e) {
            logger.error("Error reading input", e);
        } finally {
            scheduler.shutdown();
        }
    }

    private void handleCommand(String command) {
        String[] parts = command.split("\\s+");
        if (parts.length == 0) return;

        switch (parts[0].toLowerCase()) {
            case "get":
                if (parts.length != 2) {
                    System.out.println("Usage: get <key>");
                    return;
                }
                sendRequest(parts[1], null);
                break;
            case "set":
                if (parts.length != 3) {
                    System.out.println("Usage: set <key> <value>");
                    return;
                }
                sendRequest(parts[1], parts[2]);
                break;
            case "del":
                if (parts.length != 2) {
                    System.out.println("Usage: del <key>");
                    return;
                }
                sendRequest(parts[1], "DELETE");
                break;
            case "append":
                if (parts.length != 3) {
                    System.out.println("Usage: append <key> <value>");
                    return;
                }
                sendRequest(parts[1], "APPEND:" + parts[2]);
                break;
            case "strln":
                if (parts.length != 2) {
                    System.out.println("Usage: strln <key>");
                    return;
                }
                sendRequest(parts[1], "STRLEN");
                break;
            case "ping":
                pingServers();
                break;
            default:
                System.out.println("Unknown command: " + parts[0]);
        }
    }

    private void sendRequest(String key, String value) {
        if (currentLeader == null) {
            System.out.println("No leader available. Trying to discover...");
            discoverLeader();
            if (currentLeader == null) {
                System.out.println("Still no leader available. Please try again later.");
                return;
            }
        }

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(servers.get(currentLeader));
            RaftMessage request = new RaftMessage(MessageType.CLIENT_REQUEST, 0, "client", key, value);
            String jsonRequest = objectMapper.writeValueAsString(request);
            channel.write(ByteBuffer.wrap(jsonRequest.getBytes()));

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            channel.read(buffer);
            buffer.flip();
            String response = new String(buffer.array(), 0, buffer.limit());
            RaftMessage raftResponse = objectMapper.readValue(response, RaftMessage.class);

            if (raftResponse.getType() == MessageType.CLIENT_RESPONSE) {
                String result = raftResponse.getValue();
                if (result.startsWith("NOT_LEADER:")) {
                    currentLeader = result.substring(11);
                    System.out.println("Redirected to new leader: " + currentLeader);
                    sendRequest(key, value);
                } else {
                    System.out.println(result);
                }
            }
        } catch (Exception e) {
            logger.error("Error sending request", e);
            currentLeader = null;
        }
    }

    private void discoverLeader() {
        for (Map.Entry<String, InetSocketAddress> server : servers.entrySet()) {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(server.getValue());
                RaftMessage request = new RaftMessage(MessageType.CLIENT_REQUEST, 0, "client", "ping", null);
                String jsonRequest = objectMapper.writeValueAsString(request);
                channel.write(ByteBuffer.wrap(jsonRequest.getBytes()));

                ByteBuffer buffer = ByteBuffer.allocate(1024);
                channel.read(buffer);
                buffer.flip();
                String response = new String(buffer.array(), 0, buffer.limit());
                RaftMessage raftResponse = objectMapper.readValue(response, RaftMessage.class);

                if (raftResponse.getType() == MessageType.CLIENT_RESPONSE) {
                    String result = raftResponse.getValue();
                    if (!result.startsWith("NOT_LEADER:")) {
                        currentLeader = server.getKey();
                        return;
                    } else {
                        currentLeader = result.substring(11);
                        return;
                    }
                }
            } catch (Exception e) {
                logger.debug("Server {} is not responding", server.getKey());
            }
        }
    }

    private void pingServers() {
        System.out.println("Pinging all servers...");
        for (Map.Entry<String, Integer> entry : SERVER_PORTS.entrySet()) {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(1000);
                if (channel.connect(new InetSocketAddress("127.0.0.1", entry.getValue()))) {
                    System.out.println(entry.getKey() + ": OK");
                } else {
                    System.out.println(entry.getKey() + ": Not responding");
                }
            } catch (IOException e) {
                System.out.println(entry.getKey() + ": Not responding");
            }
        }
    }

    public static void main(String[] args) {
        RaftCLI cli = new RaftCLI();
        cli.start();
    }
} 