package com.raft.rpc;

import com.google.gson.annotations.SerializedName;

public class HeartbeatMessage {
    @SerializedName("type")
    private final String type = "HEARTBEAT";
    
    @SerializedName("leaderId")
    private final String leaderId;
    
    @SerializedName("term")
    private final long term;
    
    @SerializedName("timestamp")
    private final long timestamp;

    public HeartbeatMessage(String leaderId, long term) {
        this.leaderId = leaderId;
        this.term = term;
        this.timestamp = System.currentTimeMillis();
    }

    public String getType() {
        return type;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getTerm() {
        return term;
    }

    public long getTimestamp() {
        return timestamp;
    }
} 