package com.raft.rpc;

import com.google.gson.annotations.SerializedName;

public class AppendEntriesResponse {
    @SerializedName("type")
    private final String type = "APPEND_ENTRIES_RESPONSE";
    
    @SerializedName("term")
    private final long term;
    
    @SerializedName("success")
    private final boolean success;
    
    @SerializedName("followerId")
    private final String followerId;

    @SerializedName("commandId")
    private final String commandId;
    
    @SerializedName("lastLogIndex")
    private final long lastLogIndex;

    public AppendEntriesResponse(long term, boolean success, String followerId, String commandId, long lastLogIndex) {
        this.term = term;
        this.success = success;
        this.followerId = followerId;
        this.commandId = commandId;
        this.lastLogIndex = lastLogIndex;
    }

    public String getType() {
        return type;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getFollowerId() {
        return followerId;
    }

    public String getCommandId() {
        return commandId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }
}
