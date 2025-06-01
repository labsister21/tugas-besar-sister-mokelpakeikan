package com.raft.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RaftMessage {
    public enum MessageType {
        APPEND_ENTRIES,
        APPEND_ENTRIES_RESPONSE,
        REQUEST_VOTE,
        REQUEST_VOTE_RESPONSE,
        CLIENT_REQUEST,
        CLIENT_RESPONSE
    }

    private MessageType type;
    private long term;
    private String senderId;
    private long logIndex;
    private long logTerm;
    private String key;
    private String value;
    private boolean success;

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

    // Constructor for client requests
    public RaftMessage(MessageType type, long term, String senderId, String key, String value) {
        this(type, term, senderId, -1, -1, key, value, false);
    }

    // Constructor for vote requests/responses
    public RaftMessage(MessageType type, long term, String senderId, boolean success) {
        this(type, term, senderId, -1, -1, null, null, success);
    }

    // Constructor for heartbeat
    public RaftMessage(MessageType type, long term, String senderId) {
        this(type, term, senderId, -1, -1, null, null, false);
    }

    // Constructor for log replication
    public RaftMessage(MessageType type, long term, String senderId, long logIndex, long logTerm, String key, String value) {
        this(type, term, senderId, logIndex, logTerm, key, value, false);
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

    public void setType(MessageType type) {
        this.type = type;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public void setLogIndex(long logIndex) {
        this.logIndex = logIndex;
    }

    public void setLogTerm(long logTerm) {
        this.logTerm = logTerm;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
} 