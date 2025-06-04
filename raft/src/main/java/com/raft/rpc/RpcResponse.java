package com.raft.rpc;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class RpcResponse {
    @SerializedName("jsonrpc")
    private final String jsonrpc = "2.0";
    
    @SerializedName("id")
    private String id;
    
    @SerializedName("result")
    private Object result;
    
    @SerializedName("error")
    private RpcError error;

    public RpcResponse(String id, Object result) {
        this.id = id;
        this.result = result;
        this.error = null;
    }

    public RpcResponse(String id, RpcError error) {
        this.id = id;
        this.result = null;
        this.error = error;
    }

    public String getId() {
        return id;
    }

    public Object getResult() {
        return result;
    }

    public RpcError getError() {
        return error;
    }

    public String toJson() {
        return new Gson().toJson(this);
    }

    public static RpcResponse fromJson(String json) {
        return new Gson().fromJson(json, RpcResponse.class);
    }

    public static RpcResponse error(int code, String message) {
        return new RpcResponse(null, new RpcError(code, message, null));
    }

    public static RpcResponse success(String result) {
        return new RpcResponse(null, result);
    }

    public static class RpcError {
        @SerializedName("code")
        private int code;
        
        @SerializedName("message")
        private String message;
        
        @SerializedName("data")
        private Object data;

        public RpcError(int code, String message, Object data) {
            this.code = code;
            this.message = message;
            this.data = data;
        }

        public int getCode() {
            return code;
        }

        public String getMessage() {
            return message;
        }

        public Object getData() {
            return data;
        }
    }
} 