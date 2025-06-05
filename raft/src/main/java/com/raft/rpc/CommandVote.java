package com.raft.rpc;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.annotations.SerializedName;

public class CommandVote {
    @SerializedName("command")
    private final RpcMessage command;
    
    @SerializedName("votes")
    private final Set<String> votes = ConcurrentHashMap.newKeySet();
    
    @SerializedName("replicatedNodes")
    private final Set<String> replicatedNodes = ConcurrentHashMap.newKeySet();
    
    @SerializedName("timestamp")
    private final long timestamp;
    
    @SerializedName("logIndex")
    private final long logIndex;
    
    @SerializedName("executed")
    private volatile boolean executed = false;
    
    @SerializedName("committed")
    private volatile boolean committed = false;

    public CommandVote(RpcMessage command, long logIndex) {
        this.command = command;
        this.timestamp = System.currentTimeMillis();
        this.logIndex = logIndex;
    }

    public RpcMessage getCommand() {
        return command;
    }

    public Set<String> getVotes() {
        return votes;
    }

    public Set<String> getReplicatedNodes() {
        return replicatedNodes;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public boolean isExecuted() {
        return executed;
    }

    public void setExecuted(boolean executed) {
        this.executed = executed;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }
}
