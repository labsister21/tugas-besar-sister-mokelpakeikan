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

    @SerializedName("matchIndex")
    public final long matchIndex;

    public AppendEntriesResponse(long term, boolean success, String followerId, long matchIndex) {
        this.term = term;
        this.success = success;
        this.followerId = followerId;
        this.matchIndex = matchIndex;
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
    
    public long getMatchIndex() {
        return matchIndex;
    }
}
