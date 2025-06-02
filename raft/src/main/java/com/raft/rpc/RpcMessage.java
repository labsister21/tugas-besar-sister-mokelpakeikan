package com.raft.rpc;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class RpcMessage {
    @SerializedName("jsonrpc")
    private final String jsonrpc = "2.0";
    
    @SerializedName("id")
    private String id;
    
    @SerializedName("method")
    private String method;
    
    @SerializedName("params")
    private Object params;

    public RpcMessage(String id, String method, Object params) {
        this.id = id;
        this.method = method;
        this.params = params;
    }

    public String getId() {
        return id;
    }

    public String getMethod() {
        return method;
    }

    public Object getParams() {
        return params;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static RpcMessage fromJson(String json) {
        return new Gson().fromJson(json, RpcMessage.class);
    }
} 