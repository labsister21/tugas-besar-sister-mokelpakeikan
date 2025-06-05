package com.raft.rpc;

import com.google.gson.annotations.SerializedName;

public class AppendEntriesResponse {
    @SerializedName("type")
    public final String type = "APPEND_ENTRIES_RESPONSE";
    
    @SerializedName("term")
    public final long term;
    
    @SerializedName("success")
    public final boolean success;
    
    @SerializedName("followerId")
    public final String followerId;

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
