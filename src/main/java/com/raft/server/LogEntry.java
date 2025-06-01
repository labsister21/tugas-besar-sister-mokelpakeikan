package com.raft.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LogEntry {
    public enum CommandType {
        GET,
        SET,
        DEL,
        APPEND,
        STRLEN
    }

    private final long term;
    private final String key;
    private final String value;
    private final CommandType commandType;

    @JsonCreator
    public LogEntry(
            @JsonProperty("term") long term,
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("commandType") CommandType commandType) {
        this.term = term;
        this.key = key;
        this.value = value;
        this.commandType = commandType;
    }

    public long getTerm() {
        return term;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public CommandType getCommandType() {
        return commandType;
    }

    @Override
    public String toString() {
        return String.format("LogEntry{term=%d, type=%s, key='%s', value='%s'}", 
            term, commandType, key, value);
    }
} 