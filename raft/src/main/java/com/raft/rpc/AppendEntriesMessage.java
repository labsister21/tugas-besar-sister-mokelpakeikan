package com.raft.rpc;

import com.raft.node.LogEntry;

import java.io.Serializable;
import java.util.List;

public class AppendEntriesMessage implements Serializable {
    private final String type = "APPEND_ENTRIES";
    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<LogEntry> entries;
    private final long leaderCommit;

    public AppendEntriesMessage(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    // Getters
    public String getType() { return type; }
    public long getTerm() { return term; }
    public String getLeaderId() { return leaderId; }
    public long getPrevLogIndex() { return prevLogIndex; }
    public long getPrevLogTerm() { return prevLogTerm; }
    public List<LogEntry> getEntries() { return entries; }
    public long getLeaderCommit() { return leaderCommit; }
}