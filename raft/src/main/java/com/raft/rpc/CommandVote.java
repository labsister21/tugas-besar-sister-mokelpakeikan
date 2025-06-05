package com.raft.rpc;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.annotations.SerializedName;

public class CommandVote {
    @SerializedName("command")
    public final RpcMessage command;
    
    @SerializedName("votes")
    public final Set<String> votes = ConcurrentHashMap.newKeySet();
    
    @SerializedName("replicatedNodes")
    public final Set<String> replicatedNodes = ConcurrentHashMap.newKeySet();
    
    @SerializedName("timestamp")
    public final long timestamp;
    
    @SerializedName("logIndex")
    public final long logIndex;
    
    @SerializedName("executed")
    public volatile boolean executed = false;
    
    @SerializedName("committed")
    public volatile boolean committed = false;

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

    public void addVote(String voterId) {
        votes.add(voterId);
    }
}
