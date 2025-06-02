package com.raft.rpc;

import com.google.gson.annotations.SerializedName;

public class VoteMessage {
    public static class VoteRequest {
        @SerializedName("type")
        private final String type = "VOTE_REQUEST";
        
        @SerializedName("candidateId")
        private final String candidateId;
        
        @SerializedName("term")
        private final long term;
        
        @SerializedName("lastLogIndex")
        private final long lastLogIndex;
        
        @SerializedName("lastLogTerm")
        private final long lastLogTerm;

        public VoteRequest(String candidateId, long term, long lastLogIndex, long lastLogTerm) {
            this.candidateId = candidateId;
            this.term = term;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        public String getType() {
            return type;
        }

        public String getCandidateId() {
            return candidateId;
        }

        public long getTerm() {
            return term;
        }

        public long getLastLogIndex() {
            return lastLogIndex;
        }

        public long getLastLogTerm() {
            return lastLogTerm;
        }
    }

    public static class VoteResponse {
        @SerializedName("type")
        private final String type = "VOTE_RESPONSE";
        
        @SerializedName("term")
        private final long term;
        
        @SerializedName("voteGranted")
        private final boolean voteGranted;
        
        @SerializedName("voterId")
        private final String voterId;

        public VoteResponse(long term, boolean voteGranted, String voterId) {
            this.term = term;
            this.voteGranted = voteGranted;
            this.voterId = voterId;
        }

        public String getType() {
            return type;
        }

        public long getTerm() {
            return term;
        }

        public boolean isVoteGranted() {
            return voteGranted;
        }

        public String getVoterId() {
            return voterId;
        }
    }
} 