package com.raft.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class);
    private static final int HEARTBEAT_INTERVAL = 150; // milliseconds
    private static final int ELECTION_TIMEOUT_MIN = 150; // milliseconds
    private static final int ELECTION_TIMEOUT_MAX = 300; // milliseconds
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 1000;

    private final String serverId;
    private final int port;
    private final Map<String, InetSocketAddress> clusterMembers;
    private final Map<String, Long> nextIndex;
    private final Map<String, Long> matchIndex;
    private final Map<String, Long> lastHeartbeat;
    private final Map<String, String> keyValueStore;
    private final List<LogEntry> log;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService executorService;
    private final AtomicInteger votesReceived;

    private volatile ServerState state;
    private volatile String currentLeader;
    private volatile long currentTerm;
    private volatile String votedFor;
    private volatile long commitIndex;
    private volatile long lastApplied;
    private volatile long electionTimeout;
    private volatile ScheduledFuture<?> heartbeatTask;
    private volatile ScheduledFuture<?> electionTask;

    public RaftServer(String serverId, int port, Map<String, InetSocketAddress> clusterMembers) {
        this.serverId = serverId;
        this.port = port;
        this.clusterMembers = new ConcurrentHashMap<>(clusterMembers);
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        this.lastHeartbeat = new ConcurrentHashMap<>();
        this.keyValueStore = new ConcurrentHashMap<>();
        this.log = Collections.synchronizedList(new ArrayList<>());
        this.objectMapper = new ObjectMapper();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.executorService = Executors.newFixedThreadPool(10);
        this.votesReceived = new AtomicInteger(0);

        this.state = ServerState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.currentLeader = null;
        resetElectionTimeout();
    }

    public void start() {
        try {
            startServer();
            scheduleHeartbeat();
            scheduleElection();
        } catch (IOException e) {
            logger.error("Failed to start server", e);
        }
    }

    private void startServer() throws IOException {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.socket().bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);
        logger.info("Server socket bound to port {}", port);

        Selector selector = Selector.open();
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        logger.info("Server socket registered with selector");

        executorService.submit(() -> {
            try {
                logger.info("Starting server loop for {}", serverId);
                while (true) {
                    selector.select();
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> iter = selectedKeys.iterator();

                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        if (key.isAcceptable()) {
                            handleAccept(key, selector);
                        } else if (key.isReadable()) {
                            handleRead(key);
                        }
                        iter.remove();
                    }
                }
            } catch (IOException e) {
                logger.error("Error in server loop for {}", serverId, e);
            }
        });
    }

    private void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel client = serverChannel.accept();
        client.configureBlocking(false);
        client.register(selector, SelectionKey.OP_READ);
        logger.info("Accepted new connection on server {}", serverId);
    }

    private void handleRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try {
            int bytesRead = channel.read(buffer);
            if (bytesRead == -1) {
                // Connection closed by peer
                logger.info("Connection closed by peer for server {}", serverId);
                channel.close();
                key.cancel();
                return;
            }
            
            if (bytesRead > 0) {
                buffer.flip();
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                String message = new String(data);
                logger.debug("Received message on server {}: {}", serverId, message);
                handleMessage(message, channel);
            }
        } catch (IOException e) {
            logger.error("Error reading from channel for server {}: {}", serverId, e.getMessage());
            try {
                channel.close();
            } catch (IOException ex) {
                logger.error("Error closing channel for server {}: {}", serverId, ex.getMessage());
            }
            key.cancel();
        }
    }

    private void handleMessage(String message, SocketChannel channel) {
        try {
            RaftMessage raftMessage = objectMapper.readValue(message, RaftMessage.class);
            switch (raftMessage.getType()) {
                case APPEND_ENTRIES:
                    handleAppendEntries(raftMessage, channel);
                    break;
                case APPEND_ENTRIES_RESPONSE:
                    handleAppendEntriesResponse(raftMessage);
                    break;
                case REQUEST_VOTE:
                    handleRequestVote(raftMessage, channel);
                    break;
                case REQUEST_VOTE_RESPONSE:
                    handleRequestVoteResponse(raftMessage);
                    break;
                case CLIENT_REQUEST:
                    handleClientRequest(raftMessage, channel);
                    break;
                case CLIENT_RESPONSE:
                    // Client responses are not handled by servers
                    break;
            }
        } catch (IOException e) {
            logger.error("Error handling message", e);
        }
    }

    private void scheduleHeartbeat() {
        heartbeatTask = scheduler.scheduleAtFixedRate(() -> {
            if (state == ServerState.LEADER) {
                sendHeartbeat();
            }
        }, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void scheduleElection() {
        electionTask = scheduler.scheduleAtFixedRate(() -> {
            if (state != ServerState.LEADER && System.currentTimeMillis() - electionTimeout > 0) {
                startElection();
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
    }

    private void resetElectionTimeout() {
        electionTimeout = System.currentTimeMillis() + 
            new Random().nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN;
    }

    private void startElection() {
        logger.info("{} starting election for term {}", serverId, currentTerm + 1);
        currentTerm++;
        state = ServerState.CANDIDATE;
        votedFor = serverId;
        votesReceived.set(1); // Vote for self
        resetElectionTimeout();
        requestVotes();
    }

    private void requestVotes() {
        RaftMessage voteRequest = new RaftMessage(
            MessageType.REQUEST_VOTE,
            currentTerm,
            serverId,
            log.size() - 1,
            log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm()
        );

        for (Map.Entry<String, InetSocketAddress> member : clusterMembers.entrySet()) {
            if (!member.getKey().equals(serverId)) {
                sendMessage(voteRequest, member.getValue());
            }
        }
    }

    private void sendHeartbeat() {
        RaftMessage heartbeat = new RaftMessage(
            MessageType.APPEND_ENTRIES,
            currentTerm,
            serverId,
            commitIndex,
            currentTerm
        );

        for (Map.Entry<String, InetSocketAddress> member : clusterMembers.entrySet()) {
            if (!member.getKey().equals(serverId)) {
                sendMessage(heartbeat, member.getValue());
            }
        }
    }

    private void sendMessage(RaftMessage message, InetSocketAddress address) {
        executorService.submit(() -> {
            SocketChannel channel = null;
            int retries = 0;
            
            while (retries < MAX_RETRIES) {
                try {
                    channel = SocketChannel.open();
                    channel.configureBlocking(true);
                    channel.socket().setSoTimeout(5000);
                    
                    // Use IP address directly
                    String hostAddress = address.getAddress().getHostAddress();
                    InetSocketAddress directAddress = new InetSocketAddress(hostAddress, address.getPort());
                    
                    logger.info("{} attempting to connect to {}:{} (attempt {}/{})", 
                        serverId, hostAddress, address.getPort(), retries + 1, MAX_RETRIES);
                    
                    if (!channel.connect(directAddress)) {
                        throw new IOException("Failed to connect to " + directAddress);
                    }
                    
                    String jsonMessage = objectMapper.writeValueAsString(message);
                    ByteBuffer buffer = ByteBuffer.wrap(jsonMessage.getBytes());
                    while (buffer.hasRemaining()) {
                        channel.write(buffer);
                    }
                    
                    logger.info("{} successfully sent message to {}:{}", 
                        serverId, hostAddress, address.getPort());
                    return; // Success, exit the retry loop
                    
                } catch (IOException e) {
                    String hostAddress = address.getAddress().getHostAddress();
                    logger.error("{} failed to send message to {}:{} - {} (attempt {}/{})", 
                        serverId, hostAddress, address.getPort(), e.getMessage(), 
                        retries + 1, MAX_RETRIES);
                    
                    if (channel != null) {
                        try {
                            channel.close();
                        } catch (IOException ex) {
                            logger.error("Error closing channel for server {}: {}", serverId, ex.getMessage());
                        }
                    }
                    
                    retries++;
                    if (retries < MAX_RETRIES) {
                        try {
                            Thread.sleep(RETRY_DELAY_MS * retries); // Exponential backoff
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
            
            if (retries >= MAX_RETRIES) {
                String hostAddress = address.getAddress().getHostAddress();
                logger.error("{} failed to send message to {}:{} after {} attempts", 
                    serverId, hostAddress, address.getPort(), MAX_RETRIES);
            }
        });
    }

    private void handleAppendEntries(RaftMessage message, SocketChannel channel) {
        if (message.getTerm() < currentTerm) {
            sendResponse(channel, new RaftMessage(MessageType.APPEND_ENTRIES_RESPONSE, currentTerm, serverId, false));
            return;
        }

        if (message.getTerm() > currentTerm) {
            currentTerm = message.getTerm();
            state = ServerState.FOLLOWER;
            votedFor = null;
        }

        currentLeader = message.getSenderId();
        resetElectionTimeout();

        if (message.getLogIndex() == -1) {
            // Heartbeat
            sendResponse(channel, new RaftMessage(MessageType.APPEND_ENTRIES_RESPONSE, currentTerm, serverId, true));

            return;
        }

        // Log replication
        if (message.getLogIndex() < log.size()) {
            LogEntry existingEntry = log.get((int) message.getLogIndex());
            if (existingEntry.getTerm() != message.getLogTerm()) {
                // Log inconsistency, truncate log
                log.subList((int) message.getLogIndex(), log.size()).clear();
            }
        }

        if (message.getKey() != null) {
            // New log entry
            LogEntry.CommandType commandType = LogEntry.CommandType.SET;
            if (message.getValue() != null) {
                if (message.getValue().equals("DELETE")) {
                    commandType = LogEntry.CommandType.DEL;
                } else if (message.getValue().startsWith("APPEND:")) {
                    commandType = LogEntry.CommandType.APPEND;
                }
            }
            log.add(new LogEntry(currentTerm, message.getKey(), message.getValue(), commandType));
        }

        commitIndex = Math.min(message.getLogIndex(), log.size() - 1);
        applyCommittedEntries();

        sendResponse(channel, new RaftMessage(MessageType.APPEND_ENTRIES_RESPONSE, currentTerm, serverId, true));
    }

    private void handleRequestVote(RaftMessage message, SocketChannel channel) {
        logger.info("{} received vote request from {} for term {}", serverId, message.getSenderId(), message.getTerm());
        
        if (message.getTerm() < currentTerm) {
            logger.info("{} rejecting vote request from {} - term {} < current term {}", 
                serverId, message.getSenderId(), message.getTerm(), currentTerm);
            sendResponse(channel, new RaftMessage(MessageType.REQUEST_VOTE_RESPONSE, currentTerm, serverId, false));
            return;
        }

        if (message.getTerm() > currentTerm) {
            logger.info("{} updating term from {} to {}", serverId, currentTerm, message.getTerm());
            currentTerm = message.getTerm();
            state = ServerState.FOLLOWER;
            votedFor = null;
        }

        boolean voteGranted = false;
        if (votedFor == null || votedFor.equals(message.getSenderId())) {
            if (log.isEmpty() || 
                message.getLogTerm() > log.get(log.size() - 1).getTerm() ||
                (message.getLogTerm() == log.get(log.size() - 1).getTerm() && 
                 message.getLogIndex() >= log.size() - 1)) {
                votedFor = message.getSenderId();
                voteGranted = true;
                resetElectionTimeout();
                logger.info("{} granting vote to {} for term {}", serverId, message.getSenderId(), currentTerm);
            }
        }

        sendResponse(channel, new RaftMessage(MessageType.REQUEST_VOTE_RESPONSE, currentTerm, serverId, voteGranted));
    }

    private void handleRequestVoteResponse(RaftMessage message) {
        if (message.getTerm() == currentTerm && state == ServerState.CANDIDATE) {
            if (message.isSuccess()) {
                int votes = votesReceived.incrementAndGet();
                logger.info("{} received vote from {} for term {} (total votes: {})", 
                    serverId, message.getSenderId(), currentTerm, votes);
                
                // Check if we have majority
                int requiredVotes = (clusterMembers.size() + 1) / 2;
                if (votes >= requiredVotes) {
                    logger.info("{} received majority of votes for term {}, becoming leader", serverId, currentTerm);
                    becomeLeader();
                }
            }
        }
    }

    private void becomeLeader() {
        state = ServerState.LEADER;
        currentLeader = serverId;
        
        // Initialize leader state
        for (String member : clusterMembers.keySet()) {
            nextIndex.put(member, (long)log.size());
            matchIndex.put(member, 0L);
        }
        
        // Send initial heartbeat
        sendHeartbeat();
    }

    private void handleClientRequest(RaftMessage message, SocketChannel channel) {
        if (state != ServerState.LEADER) {
            // Redirect to leader if known
            if (currentLeader != null) {
                logger.info("Redirecting client request to leader: {}", currentLeader);
                sendResponse(channel, new RaftMessage(MessageType.CLIENT_RESPONSE, currentTerm, serverId, null, "NOT_LEADER:" + currentLeader));
            } else {
                logger.warn("No leader available for client request");
                sendResponse(channel, new RaftMessage(MessageType.CLIENT_RESPONSE, currentTerm, serverId, null, "NO_LEADER"));
            }
            return;
        }

        // Add to log
        LogEntry.CommandType commandType = LogEntry.CommandType.SET;
        String value = message.getValue();
        
        if (value != null) {
            if (value.equals("DELETE")) {
                commandType = LogEntry.CommandType.DEL;
            } else if (value.equals("STRLEN")) {
                commandType = LogEntry.CommandType.STRLEN;
            } else if (value.startsWith("APPEND:")) {
                commandType = LogEntry.CommandType.APPEND;
                value = value.substring(7);
            }
        } else {
            commandType = LogEntry.CommandType.GET;
        }

        LogEntry entry = new LogEntry(currentTerm, message.getKey(), value, commandType);
        log.add(entry);
        logger.info("Added new log entry: {}", entry);

        // Replicate to followers
        replicateLog();

        // Wait for majority
        waitForMajority();

        // Apply to state machine
        String result = applyCommand(entry);
        sendResponse(channel, new RaftMessage(MessageType.CLIENT_RESPONSE, currentTerm, serverId, null, result));

    }

    private void replicateLog() {
        for (Map.Entry<String, InetSocketAddress> member : clusterMembers.entrySet()) {
            if (!member.getKey().equals(serverId)) {
                long nextIdx = nextIndex.getOrDefault(member.getKey(), 0L);
                if (nextIdx < log.size()) {
                    List<LogEntry> entries = log.subList((int) nextIdx, log.size());
                    RaftMessage appendMessage = new RaftMessage(
                        MessageType.APPEND_ENTRIES,
                        currentTerm,
                        serverId,
                        nextIdx,
                        entries.get(0).getTerm(),
                        entries.get(entries.size() - 1).getKey(),
                        entries.get(entries.size() - 1).getValue(),
                        true
                    );
                    sendMessage(appendMessage, member.getValue());
                }
            }
        }
    }

    private void waitForMajority() {
        int requiredVotes = (clusterMembers.size() + 1) / 2;
        int votes = 1; // Count self

        for (Map.Entry<String, Long> match : matchIndex.entrySet()) {
            if (match.getValue() >= log.size() - 1) {
                votes++;
            }
        }

        while (votes < requiredVotes) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get((int) lastApplied);
            applyCommand(entry);
        }
    }

    private String applyCommand(LogEntry entry) {
        String result;
        switch (entry.getCommandType()) {
            case SET:
                keyValueStore.put(entry.getKey(), entry.getValue());
                result = "OK";
                logger.info("SET {} = {}", entry.getKey(), entry.getValue());
                break;
            case DEL:
                result = keyValueStore.remove(entry.getKey());
                if (result == null) result = "";
                logger.info("DEL {} = {}", entry.getKey(), result);
                break;
            case APPEND:
                String currentValue = keyValueStore.get(entry.getKey());
                String newValue = (currentValue != null ? currentValue : "") + entry.getValue();
                keyValueStore.put(entry.getKey(), newValue);
                result = "OK";
                logger.info("APPEND {} += {} (new value: {})", entry.getKey(), entry.getValue(), newValue);
                break;
            case STRLEN:
                String value = keyValueStore.get(entry.getKey());
                result = value != null ? String.valueOf(value.length()) : "0";
                logger.info("STRLEN {} = {}", entry.getKey(), result);
                break;
            default:
                result = keyValueStore.get(entry.getKey());
                if (result == null) result = "";
                logger.info("GET {} = {}", entry.getKey(), result);
        }
        return result;
    }

    private void sendResponse(SocketChannel channel, RaftMessage response) {
        try {
            String jsonResponse = objectMapper.writeValueAsString(response);
            ByteBuffer buffer = ByteBuffer.wrap(jsonResponse.getBytes());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            logger.debug("Sent response from server {}: {}", serverId, jsonResponse);
        } catch (IOException e) {
            logger.error("Error sending response from server {}: {}", serverId, e.getMessage());
            try {
                channel.close();
            } catch (IOException ex) {
                logger.error("Error closing channel for server {}: {}", serverId, ex.getMessage());
            }
        }
    }

    private void handleAppendEntriesResponse(RaftMessage message) {
        if (state == ServerState.LEADER && message.getTerm() == currentTerm) {
            String followerId = message.getSenderId();
            if (message.isSuccess()) {
                matchIndex.put(followerId, nextIndex.get(followerId));
                nextIndex.put(followerId, nextIndex.get(followerId) + 1);
                
                // Update commit index
                long newCommitIndex = commitIndex;
                for (long i = commitIndex + 1; i < log.size(); i++) {
                    int count = 1; // Count self
                    for (long match : matchIndex.values()) {
                        if (match >= i) count++;
                    }
                    if (count > (clusterMembers.size() + 1) / 2) {
                        newCommitIndex = i;
                    }
                }
                if (newCommitIndex > commitIndex) {
                    commitIndex = newCommitIndex;
                    applyCommittedEntries();
                }
            } else {
                nextIndex.put(followerId, Math.max(0, nextIndex.get(followerId) - 1));
                // Retry append entries
                replicateLog();
            }
        }
    }

    public void stop() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
        }
        if (electionTask != null) {
            electionTask.cancel(true);
        }
        scheduler.shutdown();
        executorService.shutdown();
    }
} 