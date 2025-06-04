package com.raft.node;

import java.io.Serializable;
import com.raft.rpc.RpcMessage;

public class LogEntry implements Serializable {
    private final long term;
    private final long index;
    private final String command;
    private final String key;
    private final String value;
    private boolean committed;

    public LogEntry(long term, long index, String command, String key, String value) {
        this.term = term;
        this.index = index;
        this.command = command;
        this.key = key;
        this.value = value;
        this.committed = false;
    }

    public LogEntry(long term, RpcMessage command) {
        this.term = term;
        this.index = -1; // Will be set when added to log
        this.command = command.toString();
        this.key = null;
        this.value = null;
        this.committed = false;
    }

    // Getters
    public long getTerm() { return term; }
    public long getIndex() { return index; }
    public String getCommand() { return command; }
    public String getKey() { return key; }
    public String getValue() { return value; }
    public boolean isCommitted() { return committed; }

    // Setter for committed status
    public void setCommitted(boolean committed) { this.committed = committed; }

    @Override
    public String toString() {
        return String.format("LogEntry{term=%d, index=%d, command=%s, key=%s, value=%s, committed=%b}",
                term, index, command, key, value, committed);
    }
}