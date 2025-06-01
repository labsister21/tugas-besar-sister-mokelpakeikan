package com.raft.node;

public class Log {
    private Integer index;
    private String description;
    private Integer term;

    public Log(Integer index, String description, Integer term) {
        this.index = index;
        this.description = description;
        this.term = term;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getTerm() {
        return term;
    }

    public void setTerm(Integer term) {
        this.term = term;
    }

}
