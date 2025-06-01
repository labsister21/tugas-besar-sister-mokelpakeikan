package com.raft.common;

public enum MessageType {
    // Raft protocol messages
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,
    HEARTBEAT,
    
    // Membership change messages
    MEMBERSHIP_CHANGE_REQUEST,
    MEMBERSHIP_CHANGE_RESPONSE,
    
    // Client messages
    CLIENT_REQUEST,
    CLIENT_RESPONSE,
    
    // Log replication messages
    LOG_SYNC_REQUEST,
    LOG_SYNC_RESPONSE
} 