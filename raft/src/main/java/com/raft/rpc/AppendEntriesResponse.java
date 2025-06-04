package com.raft.rpc;

import java.io.Serializable;

public class AppendEntriesResponse implements Serializable {
    private final String type = "APPEND_ENTRIES_RESPONSE";
    private final long term;
    private final boolean success;
    private final String followerId;
    private final long matchIndex;

    public AppendEntriesResponse(long term, boolean success, String followerId, long matchIndex) {
        this.term = term;
        this.success = success;
        this.followerId = followerId;
        this.matchIndex = matchIndex;
    }

    // Getters
    public String getType() { return type; }
    public long getTerm() { return term; }
    public boolean isSuccess() { return success; }
    public String getFollowerId() { return followerId; }
    public long getMatchIndex() { return matchIndex; }
}