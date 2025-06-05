package com.raft.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.raft.node.LogEntry;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PersistentState {
    private static final String STORAGE_DIR = "storage";
    private static final String STATE_FILE = "state.json";
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    static {
        // Create storage directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(STORAGE_DIR));
        } catch (IOException e) {
            System.err.println("Failed to create storage directory: " + e.getMessage());
        }
    }

    public static class State {
        private long currentTerm;
        private String votedFor;
        private List<LogEntry> log;

        public State(long currentTerm, String votedFor, List<LogEntry> log) {
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.log = log;
        }

        // Getters and setters
        public long getCurrentTerm() { return currentTerm; }
        public void setCurrentTerm(long currentTerm) { this.currentTerm = currentTerm; }
        public String getVotedFor() { return votedFor; }
        public void setVotedFor(String votedFor) { this.votedFor = votedFor; }
        public List<LogEntry> getLog() { return log; }
        public void setLog(List<LogEntry> log) { this.log = log; }
    }

    public static synchronized void saveState(long currentTerm, String votedFor, List<LogEntry> log) {
        lock.writeLock().lock();
        try {
            State state = new State(currentTerm, votedFor, log);
            String json = gson.toJson(state);
            Path filePath = Paths.get(STORAGE_DIR, STATE_FILE);
            
            // Write to temporary file first
            Path tempFile = Files.createTempFile(Paths.get(STORAGE_DIR), "state", ".tmp");
            Files.write(tempFile, json.getBytes());
            
            // Atomic move to final location
            Files.move(tempFile, filePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            
        } catch (IOException e) {
            System.err.println("Failed to save state: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static synchronized State loadState() {
        lock.readLock().lock();
        try {
            Path filePath = Paths.get(STORAGE_DIR, STATE_FILE);
            if (!Files.exists(filePath)) {
                return new State(0, null, List.of());
            }

            String json = Files.readString(filePath);
            return gson.fromJson(json, State.class);
        } catch (IOException e) {
            System.err.println("Failed to load state: " + e.getMessage());
            return new State(0, null, List.of());
        } finally {
            lock.readLock().unlock();
        }
    }
} 