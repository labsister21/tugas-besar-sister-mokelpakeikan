package com.rafted;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class NetworkManager {
    private static final Logger logger = Logger.getLogger(NetworkManager.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final ServerSocket serverSocket;
    private final ExecutorService threadPool;
    private final RaftNode node;
    private volatile boolean running = true;
    
    public NetworkManager(RaftNode node, int port) throws IOException {
        this.node = node;
        this.serverSocket = new ServerSocket(port);
        this.threadPool = Executors.newCachedThreadPool();
        startServer();
    }
    
    private void startServer() {
        threadPool.submit(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleClientConnection(clientSocket);
                } catch (IOException e) {
                    if (running) {
                        logger.severe("Error accepting client connection: " + e.getMessage());
                    }
                }
            }
        });
    }
    
    private void handleClientConnection(Socket clientSocket) {
        threadPool.submit(() -> {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {
                
                String message = in.readLine();
                if (message != null) {
                    Message request = objectMapper.readValue(message, Message.class);
                    Message response = handleRequest(request);
                    out.println(objectMapper.writeValueAsString(response));
                }
            } catch (IOException e) {
                logger.severe("Error handling client connection: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    logger.warning("Error closing client socket: " + e.getMessage());
                }
            }
        });
    }
    
    private Message handleRequest(Message request) {
        try {
            switch (request.getType()) {
                case REQUEST_VOTE:
                    return handleRequestVote(request);
                case APPEND_ENTRIES:
                    return handleAppendEntries(request);
                case CLIENT_COMMAND:
                    return handleClientCommand(request);
                default:
                    return new Message(MessageType.ERROR, "Unknown message type");
            }
        } catch (Exception e) {
            logger.severe("Error handling request: " + e.getMessage());
            return new Message(MessageType.ERROR, e.getMessage());
        }
    }
    
    private Message handleRequestVote(Message request) {
        try {
            RaftNode.RequestVoteRequest voteRequest = objectMapper.convertValue(request.getData(), RaftNode.RequestVoteRequest.class);
            
            // If request term is less than current term, reject
            if (voteRequest.getTerm() < node.getCurrentTerm()) {
                return new Message(MessageType.REQUEST_VOTE_RESPONSE, node.getCurrentTerm());
            }
            
            // If request term is greater than current term, update term and become follower
            if (voteRequest.getTerm() > node.getCurrentTerm()) {
                node.updateTerm(voteRequest.getTerm());
                node.setState(RaftNode.State.FOLLOWER);
                node.setVotedFor(-1); // Reset votedFor when we see a new term
            }
            
            // If already voted for someone else in this term, reject
            if (node.getVotedFor() != -1 && node.getVotedFor() != voteRequest.getCandidateId()) {
                return new Message(MessageType.REQUEST_VOTE_RESPONSE, "Already voted");
            }
            
            // Check if candidate's log is at least as up-to-date as receiver's log
            if (!node.isLogUpToDate(voteRequest.getLastLogIndex(), voteRequest.getLastLogTerm())) {
                return new Message(MessageType.REQUEST_VOTE_RESPONSE, "Log not up to date");
            }
            
            // Grant vote
            node.setVotedFor(voteRequest.getCandidateId());
            node.setState(RaftNode.State.FOLLOWER); // Become follower when granting vote
            return new Message(MessageType.REQUEST_VOTE_RESPONSE, "Vote granted");
        } catch (Exception e) {
            logger.severe("Error handling vote request: " + e.getMessage());
            return new Message(MessageType.ERROR, e.getMessage());
        }
    }
    
    private Message handleAppendEntries(Message request) {
        try {
            RaftNode.AppendEntriesRequest appendRequest = objectMapper.convertValue(request.getData(), RaftNode.AppendEntriesRequest.class);
            node.setLeaderId(appendRequest.getLeaderId());
            
            if (appendRequest.getTerm() < node.getCurrentTerm()) {
                return new Message(MessageType.APPEND_ENTRIES_RESPONSE, node.getCurrentTerm());
            }
            
            // If leader's term is greater than or equal to current term, become follower
            if (appendRequest.getTerm() >= node.getCurrentTerm()) {
                node.updateTerm(appendRequest.getTerm());
                node.setState(RaftNode.State.FOLLOWER);
                node.setVotedFor(-1); // Reset votedFor when we see a new term
               
            }
            
            // Reset election timer since we received a valid heartbeat
            node.resetElectionTimer();
            
            // For now, just accept the heartbeat
            // TODO: Implement proper log replication
            return new Message(MessageType.APPEND_ENTRIES_RESPONSE, node.getCurrentTerm());
        } catch (Exception e) {
            logger.severe("Error handling append entries: " + e.getMessage());
            return new Message(MessageType.ERROR, e.getMessage());
        }
    }
    
    private Message handleClientCommand(Message request) {
        try {
            // First check if we're the leader
            if (node.getState() != RaftNode.State.LEADER) {
                // If not the leader, forward the command to the known leader
                String leaderAddress = node.getLeaderAddress();
                int leaderPort = node.getLeaderPort();

                if (leaderAddress != null && leaderPort != -1) {
                    try (Socket leaderSocket = new Socket(leaderAddress, leaderPort);
                         PrintWriter outToLeader = new PrintWriter(leaderSocket.getOutputStream(), true);
                         BufferedReader inFromLeader = new BufferedReader(new InputStreamReader(leaderSocket.getInputStream()))) {

                        // Forward the client command message to the leader
                        outToLeader.println(objectMapper.writeValueAsString(request));

                        // Get the response from the leader
                        String responseStr = inFromLeader.readLine();
                        if (responseStr != null) {
                            // Return the leader's response to the original client
                            return objectMapper.readValue(responseStr, Message.class);
                        } else {
                            return new Message(MessageType.ERROR, "Leader did not respond");
                        }
                    } catch (IOException e) {
                        logger.warning("Failed to forward command to leader " + leaderAddress + ":" + leaderPort + ": " + e.getMessage());
                        return new Message(MessageType.ERROR, "Failed to connect to leader");
                    }
                } else {
                    return new Message(MessageType.NOT_LEADER, "Not the leader. Leader is unknown.");
                }
            }

            // If we are the leader, execute the command locally
            Command command = objectMapper.convertValue(request.getData(), Command.class);
            String result = executeCommand(command);
            // TODO: Implement log replication before returning response
            return new Message(MessageType.CLIENT_RESPONSE, result);
        } catch (RaftNode.NotLeaderException e) {
            // This exception should ideally not be caught here after the leader check,
            // but keeping it for robustness.
            return new Message(MessageType.NOT_LEADER, e.getMessage());
        } catch (Exception e) {
            logger.severe("Error handling client command: " + e.getMessage());
            return new Message(MessageType.ERROR, e.getMessage());
        }
    }
    
    private String executeCommand(Command command) {
        switch (command.getType()) {
            case GET:
                return node.get(command.getKey());
            case SET:
                node.set(command.getKey(), command.getValue());
                return "OK";
            case DELETE:
                return node.delete(command.getKey());
            case STRLEN:
                return String.valueOf(node.getStringLength(command.getKey()));
            case APPEND:
                node.append(command.getKey(), command.getValue());
                return "OK";
            case PING:
                return "PONG";
            default:
                throw new IllegalArgumentException("Unknown command type: " + command.getType());
        }
    }
    
    public void sendMessage(String address, int port, Message message) {
        threadPool.submit(() -> {
            try (Socket socket = new Socket(address, port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                
                out.println(objectMapper.writeValueAsString(message));
                String response = in.readLine();
                if (response != null) {
                    Message responseMessage = objectMapper.readValue(response, Message.class);
                    handleResponse(responseMessage);
                }
            } catch (IOException e) {
                logger.severe("Error sending message: " + e.getMessage());
            }
        });
    }
    
    private void handleResponse(Message response) {
        // TODO: Implement response handling
        logger.info("Received response: " + response.getType());
    }
    
    public void shutdown() {
        running = false;
        threadPool.shutdown();
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.warning("Error closing server socket: " + e.getMessage());
        }
    }
    
    public static class Message {
        private MessageType type;
        private Object data;
        
        public Message() {}
        
        public Message(MessageType type, Object data) {
            this.type = type;
            this.data = data;
        }
        
        public MessageType getType() {
            return type;
        }
        
        public Object getData() {
            return data;
        }
    }
    
    public enum MessageType {
        REQUEST_VOTE,
        REQUEST_VOTE_RESPONSE,
        APPEND_ENTRIES,
        APPEND_ENTRIES_RESPONSE,
        CLIENT_COMMAND,
        CLIENT_RESPONSE,
        NOT_LEADER,
        ERROR
    }
    
    public static class Command {
        private CommandType type;
        private String key;
        private String value;
        
        public Command() {}
        
        public Command(CommandType type, String key, String value) {
            this.type = type;
            this.key = key;
            this.value = value;
        }
        
        public CommandType getType() {
            return type;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
    }
    
    public enum CommandType {
        GET,
        SET,
        DELETE,
        STRLEN,
        APPEND,
        PING
    }

    public boolean isLeader() {
        return node.getState() == RaftNode.State.LEADER;
    }

} 