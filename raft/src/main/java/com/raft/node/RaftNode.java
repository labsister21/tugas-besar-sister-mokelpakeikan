package com.raft.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;


public class RaftNode {
    private InetAddress address;
    private int port;
    private NodeType nodeType;
    private List<Log> logs;
    private ServerSocket serverSocket;
    private Dictionary<String, String> dataStore; // State machine
    private volatile boolean running = true;

    // Informasi leader (untuk redirect) - ini akan diupdate oleh algoritma Raft
    private String leaderHost = null; // Misalnya "localhost" atau IP
    private int leaderPort = 0;


    public RaftNode(String hostAddress, int port, NodeType nodeType) throws IOException {
        this.address = InetAddress.getByName(hostAddress);
        this.port = port;
        this.nodeType = nodeType;
        this.logs = new ArrayList<>();
        this.dataStore = new Hashtable<>(); // Inisialisasi state machine
        this.serverSocket = new ServerSocket(this.port, 50, this.address);
        System.out.printf("RaftNode (%s) berjalan di %s:%d%n", nodeType, this.address.getHostAddress(), this.port);

        // Inisialisasi leader hint (jika node ini adalah leader awal atau tahu siapa leader)
        if (nodeType == NodeType.LEADER) {
            this.leaderHost = this.address.getHostAddress();
            this.leaderPort = this.port;
        }
    }

    public void startServer() {
        System.out.println("Server RaftNode mulai mendengarkan koneksi...");
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Klien terhubung: " + clientSocket.getRemoteSocketAddress());
                // Buat thread baru untuk menangani klien ini
                new ClientHandler(clientSocket, this).start();
            } catch (IOException e) {
                if (!running) {
                    System.out.println("Server socket ditutup.");
                    break;
                }
                System.err.println("Error saat menerima koneksi klien: " + e.getMessage());
            }
        }
    }

    public void stopServer() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            System.err.println("Error saat menutup server socket: " + e.getMessage());
        }
    }

    // Metode untuk memproses perintah dari klien
    // Ini akan sangat disederhanakan dan tidak melibatkan logika Raft penuh
    private String processClientCommand(String commandLine) {
        String[] parts = commandLine.trim().split("\\s+", 3);
        String command = parts[0].toUpperCase();

        // Semua operasi tulis harus ke Leader
        if (!command.equals("PING") && !command.equals("GET") && nodeType != NodeType.LEADER) {
            if (leaderHost != null && leaderPort != 0) {
                return "NOT_LEADER " + leaderHost + ":" + leaderPort;
            } else {
                return "NOT_LEADER UNKNOWN"; // Leader belum diketahui
            }
        }

        switch (command) {
            case "PING":
                return "PONG";
            case "SET":
                if (parts.length == 3) {
                    // Dalam Raft nyata: log, replicate, commit, then apply
                    dataStore.put(parts[1], parts[2]);
                    return "OK";
                }
                return "ERROR: Penggunaan SET <key> <value>";
            case "GET":
                if (parts.length == 2) {
                    String value = dataStore.get(parts[1]);
                    return value != null ? "VALUE " + value : "NIL";
                }
                return "ERROR: Penggunaan GET <key>";
            // Tambahkan case untuk APPEND, DEL, STRLEN
            default:
                return "ERROR: Perintah tidak dikenal";
        }
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    // Inner class untuk menangani setiap koneksi klien dalam thread terpisah
    private static class ClientHandler extends Thread {
        private Socket clientSocket;
        private RaftNode raftNode;
        private PrintWriter out;
        private BufferedReader in;

        public ClientHandler(Socket socket, RaftNode node) {
            this.clientSocket = socket;
            this.raftNode = node;
        }

        public void run() {
            try {
                out = new PrintWriter(clientSocket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    System.out.println("Diterima dari klien " + clientSocket.getRemoteSocketAddress() + ": " + inputLine);
                    if ("QUIT".equalsIgnoreCase(inputLine)) { // Tambahkan perintah QUIT untuk klien
                        out.println("BYE");
                        break;
                    }
                    String response = raftNode.processClientCommand(inputLine);
                    out.println(response);
                }
            } catch (IOException e) {
                System.err.println("Error pada ClientHandler: " + e.getMessage());
            } finally {
                try {
                    if (in != null) in.close();
                    if (out != null) out.close();
                    if (clientSocket != null) clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error saat menutup stream/socket klien: " + e.getMessage());
                }
                System.out.println("Koneksi dengan klien " + clientSocket.getRemoteSocketAddress() + " ditutup.");
            }
        }
    }
}
