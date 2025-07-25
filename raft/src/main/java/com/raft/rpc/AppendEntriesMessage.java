package com.raft.rpc;

import java.io.Serial;
import java.util.List;
import com.raft.node.LogEntry;

import com.google.gson.annotations.SerializedName;

public class AppendEntriesMessage {
    @SerializedName("type")
    public final String type = "APPEND_ENTRIES";
    
    @SerializedName("leaderId")
    public final String leaderId;
    
    @SerializedName("term")
    public final long term;
    
    @SerializedName("command")
    public final RpcMessage command;
    
    @SerializedName("prevLogIndex")
    public final long prevLogIndex;
    
    @SerializedName("prevLogTerm")
    public final long prevLogTerm;

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