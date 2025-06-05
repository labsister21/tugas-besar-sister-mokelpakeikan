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

    public AppendEntriesResponse(long term, boolean success, String followerId) {
        this.term = term;
        this.success = success;
        this.followerId = followerId;
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
}
