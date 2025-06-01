package com.raft.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftMessage {
    private final MessageType type;
    private final long term;
    private final String senderId;
    private final long logIndex;
    private final long logTerm;
    private final String key;
    private final String value;
    private final boolean success;

    @JsonCreator
    public RaftMessage(
            @JsonProperty("type") MessageType type,
            @JsonProperty("term") long term,
            @JsonProperty("senderId") String senderId,
            @JsonProperty("logIndex") long logIndex,
            @JsonProperty("logTerm") long logTerm,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("success") boolean success) {
        this.type = type;
        this.term = term;
        this.senderId = senderId;
        this.logIndex = logIndex;
        this.logTerm = logTerm;
        this.key = key;
        this.value = value;
        this.success = success;
    }

    // Constructor for simple messages (heartbeat, vote requests)
    public RaftMessage(MessageType type, long term, String senderId, long logIndex, long logTerm) {
        this(type, term, senderId, logIndex, logTerm, null, null, false);
    }

    // Constructor for client requests
    public RaftMessage(MessageType type, long term, String senderId, String key, String value) {
        this(type, term, senderId, -1, -1, key, value, false);
    }

    // Constructor for responses
    public RaftMessage(MessageType type, long term, String senderId, boolean success) {
        this(type, term, senderId, -1, -1, null, null, success);
    }

    public MessageType getType() {
        return type;
    }

    public long getTerm() {
        return term;
    }

    public String getSenderId() {
        return senderId;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public long getLogTerm() {
        return logTerm;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isSuccess() {
        return success;
    }
} 