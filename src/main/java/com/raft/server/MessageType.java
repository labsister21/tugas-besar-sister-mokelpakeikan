package com.raft.server;

public enum MessageType {
    APPEND_ENTRIES,
    REQUEST_VOTE,
    REQUEST_VOTE_RESPONSE,
    APPEND_ENTRIES_RESPONSE,
    CLIENT_REQUEST,
    CLIENT_RESPONSE
} 