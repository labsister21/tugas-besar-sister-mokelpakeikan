package com.raft.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Config {
    private static final boolean DOCKER_MODE = System.getenv("DOCKER_MODE") != null && System.getenv("DOCKER_MODE").equals("true");
    private static final String LOCAL_IP = "127.0.0.1";
    
    public static String getHostname(String serverId) {
        if (DOCKER_MODE) {
            return serverId;
        }
        try {
            // Force resolution to localhost IP
            InetAddress.getByName(LOCAL_IP);
            return LOCAL_IP;
        } catch (UnknownHostException e) {
            return LOCAL_IP;
        }
    }
    
    public static boolean isDockerMode() {
        return DOCKER_MODE;
    }
} 