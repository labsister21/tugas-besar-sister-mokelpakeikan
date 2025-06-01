package com.rafted;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.net.Socket;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftNode {
    private static final Logger logger = Logger.getLogger(RaftNode.class.getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Raft state
    public enum State { FOLLOWER, CANDIDATE, LEADER }
    private final AtomicReference<State> state = new AtomicReference<>(State.FOLLOWER);
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private final AtomicInteger votedFor = new AtomicInteger(-1);
    private final List<LogEntry> log = new ArrayList<>();
    
    // Added field to store the current leader's ID
    private final AtomicInteger leaderId = new AtomicInteger(-1);
    
    // Node configuration
    private final int nodeId;
    private final String address;
    private final int port;
    private final Map<Integer, NodeInfo> clusterNodes = new ConcurrentHashMap<>();
    
    // Volatile state
    private int commitIndex = 0;
    private int lastApplied = 0;
    
    // Leader state
    private final Map<Integer, Integer> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> matchIndex = new ConcurrentHashMap<>();
    
    // Timers
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    
    // Key-value store
    private final Map<String, String> keyValueStore = new ConcurrentHashMap<>();
    
    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    
    
    public RaftNode(int nodeId, String address, int port, List<NodeInfo> allNodes) {
        this.nodeId = nodeId;
        this.address = address;
        this.port = port;
        this.scheduler = Executors.newScheduledThreadPool(2);
        
        // Add all nodes to cluster nodes
        for (NodeInfo nodeInfo : allNodes) {
            clusterNodes.put(nodeInfo.getNodeId(), nodeInfo);
        }
        
        // Start election timer immediately
        startElectionTimer();
    }
    
    private void startElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        
        // Random timeout between 150-300ms
        int timeout = new Random().nextInt(150) + 150;
        electionTimer = scheduler.schedule(() -> {
            if (state.get() != State.LEADER) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }
    
    private void startElection() {
        // If we're already a leader, don't start an election
        if (state.get() == State.LEADER) {
            return;
        }
        
        state.set(State.CANDIDATE);
        currentTerm.incrementAndGet();
        votedFor.set(nodeId);
        
        // Request votes from all other nodes
        RequestVoteRequest request = new RequestVoteRequest(
            currentTerm.get(),
            nodeId,
            log.size() - 1,
            log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm()
        );
        
        // Send request votes to all nodes
        AtomicInteger votesReceived = new AtomicInteger(1); // Count self vote
        int totalNodes = clusterNodes.size(); // Total nodes in cluster
        CountDownLatch latch = new CountDownLatch(clusterNodes.size() - 1);
        
        for (NodeInfo node : clusterNodes.values()) {
            if (node.getNodeId() == nodeId) continue; // Skip self
            
            threadPool.submit(() -> {
                try {
                    Socket socket = new Socket(node.getAddress(), node.getPort());
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    
                    // Send request vote
                    NetworkManager.Message message = new NetworkManager.Message(
                        NetworkManager.MessageType.REQUEST_VOTE,
                        request
                    );
                    out.println(objectMapper.writeValueAsString(message));
                    
                    // Get response
                    String responseStr = in.readLine();
                    if (responseStr != null) {
                        NetworkManager.Message response = objectMapper.readValue(responseStr, NetworkManager.Message.class);
                        if (response.getType() == NetworkManager.MessageType.REQUEST_VOTE_RESPONSE) {
                            String responseData = (String) response.getData();
                            if ("Vote granted".equals(responseData)) {
                                votesReceived.incrementAndGet();
                            }
                        }
                    }
                    
                    socket.close();
                } catch (Exception e) {
                    logger.warning("Failed to request vote from node " + node.getNodeId() + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        
        try {
            // Wait for all vote requests to complete or timeout
            if (!latch.await(150, TimeUnit.MILLISECONDS)) {
                logger.warning("Vote request timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // Check if we won the election
        if (votesReceived.get() > totalNodes / 2 && state.get() == State.CANDIDATE) {
            becomeLeader();
        } else {
            // If we didn't win, start a new election timer
            startElectionTimer();
        }
    }
    
    private void becomeLeader() {
        // Double check we're still a candidate and have the highest term
        if (state.get() != State.CANDIDATE) {
            return;
        }
        
        state.set(State.LEADER);
        logger.info("Became leader for term " + currentTerm.get());
        
        // Initialize leader state
        for (NodeInfo node : clusterNodes.values()) {
            nextIndex.put(node.getNodeId(), log.size());
            matchIndex.put(node.getNodeId(), 0);
        }
        
        // Start sending heartbeats
        startHeartbeat();
    }
    
    public void startHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
        
        // Send heartbeats every 50ms
        heartbeatTimer = scheduler.scheduleAtFixedRate(() -> {
            if (state.get() == State.LEADER) {
                sendHeartbeat();
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }
    
    private void sendHeartbeat() {
        // Double check we're still the leader
        if (state.get() != State.LEADER) {
            return;
        }
        
        for (NodeInfo node : clusterNodes.values()) {
            if (node.getNodeId() == nodeId) continue; // Skip self
            
            try {
                Socket socket = new Socket(node.getAddress(), node.getPort());
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                
                // Create append entries message
                NetworkManager.Message message = new NetworkManager.Message(
                    NetworkManager.MessageType.APPEND_ENTRIES,
                    new AppendEntriesRequest(currentTerm.get(), nodeId, 0, 0, new ArrayList<>(), 0)
                );
                
                out.println(objectMapper.writeValueAsString(message));
                
                // Get response
                String responseStr = in.readLine();
                if (responseStr != null) {
                    NetworkManager.Message response = objectMapper.readValue(responseStr, NetworkManager.Message.class);
                    if (response.getType() == NetworkManager.MessageType.APPEND_ENTRIES_RESPONSE) {
                        // If follower's term is higher, step down
                        if (response.getData() instanceof Integer && (Integer)response.getData() > currentTerm.get()) {
                            updateTerm((Integer)response.getData());
                            state.set(State.FOLLOWER);
                            return;
                        }
                    }
                }
                
                socket.close();
            } catch (Exception e) {
                logger.warning("Failed to send heartbeat to node " + node.getNodeId() + ": " + e.getMessage());
            }
        }
    }
    
    public void addNode(NodeInfo node) {
        clusterNodes.put(node.getNodeId(), node);
        if (state.get() == State.LEADER) {
            nextIndex.put(node.getNodeId(), log.size());
            matchIndex.put(node.getNodeId(), 0);
        }
    }
    
    public void removeNode(int nodeId) {
        clusterNodes.remove(nodeId);
        if (state.get() == State.LEADER) {
            nextIndex.remove(nodeId);
            matchIndex.remove(nodeId);
        }
    }
    
    // Key-value store operations
    public String get(String key) {
        if (state.get() != State.LEADER) {
            throw new NotLeaderException("Not the leader");
        }
        return keyValueStore.getOrDefault(key, "");
    }
    
    public void set(String key, String value) {
        if (state.get() != State.LEADER) {
            throw new NotLeaderException("Not the leader");
        }
        // TODO: Implement log replication for set operation
        
        keyValueStore.put(key, value);
    }
    
    public String delete(String key) {
        if (state.get() != State.LEADER) {
            throw new NotLeaderException("Not the leader");
        }
        // TODO: Implement log replication for delete operation
        return keyValueStore.remove(key);
    }
    
    public int getStringLength(String key) {
        if (state.get() != State.LEADER) {
            throw new NotLeaderException("Not the leader");
        }
        String value = keyValueStore.get(key);
        return value != null ? value.length() : 0;
    }
    
    public void append(String key, String value) {
        if (state.get() != State.LEADER) {
            throw new NotLeaderException("Not the leader");
        }
        // TODO: Implement log replication for append operation
        keyValueStore.merge(key, value, String::concat);
    }
    
    public static class NotLeaderException extends RuntimeException {
        public NotLeaderException(String message) {
            super(message);
        }
    }
    
    public static class LogEntry {
        private int term;
        private String command;
        private String key;
        private String value;

        public LogEntry() {}

        @JsonCreator
        public LogEntry(@JsonProperty("term") int term,
                        @JsonProperty("command") String command,
                        @JsonProperty("key") String key,
                        @JsonProperty("value") String value) {
            this.term = term;
            this.command = command;
            this.key = key;
            this.value = value;
        }
        
        public int getTerm() {
            return term;
        }
        
        public String getCommand() {
            return command;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
    }
    
    public static class NodeInfo {
        private final int nodeId;
        private final String address;
        private final int port;
        
        public NodeInfo(int nodeId, String address, int port) {
            this.nodeId = nodeId;
            this.address = address;
            this.port = port;
        }
        
        public int getNodeId() {
            return nodeId;
        }
        
        public String getAddress() {
            return address;
        }
        
        public int getPort() {
            return port;
        }
    }
    
    public static class RequestVoteRequest {
        private int term;
        private int candidateId;
        private int lastLogIndex;
        private int lastLogTerm;

        public RequestVoteRequest() {}

        @JsonCreator
        public RequestVoteRequest(@JsonProperty("term") int term,
                                 @JsonProperty("candidateId") int candidateId,
                                 @JsonProperty("lastLogIndex") int lastLogIndex,
                                 @JsonProperty("lastLogTerm") int lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }
        
        // Getters
        public int getTerm() { return term; }
        public int getCandidateId() { return candidateId; }
        public int getLastLogIndex() { return lastLogIndex; }
        public int getLastLogTerm() { return lastLogTerm; }
    }
    
    public static class AppendEntriesRequest {
        private int term;
        private int leaderId;
        private int prevLogIndex;
        private int prevLogTerm;
        private List<LogEntry> entries;
        private int leaderCommit;

        public AppendEntriesRequest() {}

        @JsonCreator
        public AppendEntriesRequest(@JsonProperty("term") int term,
                                   @JsonProperty("leaderId") int leaderId,
                                   @JsonProperty("prevLogIndex") int prevLogIndex,
                                   @JsonProperty("prevLogTerm") int prevLogTerm,
                                   @JsonProperty("entries") List<LogEntry> entries,
                                   @JsonProperty("leaderCommit") int leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }
        
        // Getters
        public int getTerm() { return term; }
        public int getLeaderId() { return leaderId; }
        public int getPrevLogIndex() { return prevLogIndex; }
        public int getPrevLogTerm() { return prevLogTerm; }
        public List<LogEntry> getEntries() { return entries; }
        public int getLeaderCommit() { return leaderCommit; }
    }
    
    public int getCurrentTerm() {
        return currentTerm.get();
    }
    
    public void updateTerm(int newTerm) {
        currentTerm.set(newTerm);
        votedFor.set(-1); // Reset votedFor when term changes
        leaderId.set(-1); // Reset leaderId when term changes
    }
    
    public int getVotedFor() {
        return votedFor.get();
    }
    
    public void setVotedFor(int candidateId) {
        votedFor.set(candidateId);
    }
    
    public void setState(State newState) {
        state.set(newState);
    }
    
    public boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        if (log.isEmpty()) {
            return true;
        }
        
        LogEntry lastEntry = log.get(log.size() - 1);
        if (lastLogTerm > lastEntry.getTerm()) {
            return true;
        }
        if (lastLogTerm == lastEntry.getTerm()) {
            return lastLogIndex >= log.size() - 1;
        }
        return false;
    }
    
    public void resetElectionTimer() {
        startElectionTimer();
    }
    
    public State getState() {
        return state.get();
    }

    // Method to update the known leader ID
    public void setLeaderId(int leaderId) {
        this.leaderId.set(leaderId);
    }
    
    // Methods to get the current leader's address and port
    public String getLeaderAddress() {
        int currentLeaderId = leaderId.get();
        if (currentLeaderId != -1 && clusterNodes.containsKey(currentLeaderId)) {
            return clusterNodes.get(currentLeaderId).getAddress();
        } else {
            return null; // Or throw an exception, depending on desired behavior
        }
    }
    
    public int getLeaderPort() {
        int currentLeaderId = leaderId.get();
        if (currentLeaderId != -1 && clusterNodes.containsKey(currentLeaderId)) {
            return clusterNodes.get(currentLeaderId).getPort();
        } else {
            return -1; // Or throw an exception
        }
    }
} 