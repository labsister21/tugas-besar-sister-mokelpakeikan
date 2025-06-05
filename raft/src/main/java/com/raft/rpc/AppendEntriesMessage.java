package com.raft.rpc;

import com.google.gson.annotations.SerializedName;

public class AppendEntriesMessage {
    @SerializedName("type")
    private final String type = "APPEND_ENTRIES";
    
    @SerializedName("leaderId")
    private final String leaderId;
    
    @SerializedName("term")
    private final long term;
    
    @SerializedName("command")
    private final RpcMessage command;
    
    @SerializedName("prevLogIndex")
    private final long prevLogIndex;
    
    @SerializedName("prevLogTerm")
    private final long prevLogTerm;

    public AppendEntriesMessage(String leaderId, long term, RpcMessage command, long prevLogIndex, long prevLogTerm) {
        this.leaderId = leaderId;
        this.term = term;
        this.command = command;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
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

    public RpcMessage getCommand() {
        return command;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }
}