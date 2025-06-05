package com.raft.node;

import java.util.Map;

public class LogEntry {
    private final String method;
    private final String commandId;
    private final long logIndex;
    private final long term;
    private final Map<String, Object> params;

    public LogEntry(String method, String commandId, long logIndex, long term, Map<String, Object> params) {
        this.method = method;
        this.commandId = commandId;
        this.logIndex = logIndex;
        this.term = term;
        this.params = params;
    }

    public String getMethod() {
        return method;
    }

    public String getCommandId() {
        return commandId;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public long getTerm() {
        return term;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    @Override
    public String toString() {
        return String.format("[Index: %d, Term: %d] %s (ID: %s) %s", 
            logIndex, term, method, commandId, params);
    }
} 