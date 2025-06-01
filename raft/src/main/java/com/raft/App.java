package com.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class App {
    private Socket clientSocket;
    private PrintWriter out;
    private BufferedReader in;
    private String currentLeaderHost;
    private int currentLeaderPort;

    public App(String initialHost, int initialPort) {
        this.currentLeaderHost = initialHost;
        this.currentLeaderPort = initialPort;
    }

    public boolean connect() {
        try {
            System.out.printf("Mencoba terhubung ke %s:%d...%n", currentLeaderHost, currentLeaderPort);
            clientSocket = new Socket(currentLeaderHost, currentLeaderPort);
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            System.out.println("Terhubung ke server.");
            return true;
        } catch (UnknownHostException e) {
            System.err.println("Host tidak diketahui: " + currentLeaderHost);
            return false;
        } catch (IOException e) {
            System.err.println("Tidak bisa mendapatkan I/O untuk koneksi ke " + currentLeaderHost + ":" + currentLeaderPort);
            return false;
        }
    }

    private String sendCommandAndGetResponse(String command) {
        if (clientSocket == null || clientSocket.isClosed()) {
            if (!connect()) {
                return "ERROR: Tidak dapat terhubung ke server.";
            }
        }
        try {
            out.println(command);
            String resp = in.readLine();
            if (resp != null && resp.startsWith("NOT_LEADER")) {
                System.out.println("Server merespons: " + resp);
                handleRedirection(resp);
                // Coba kirim ulang perintah setelah redirect (ini bisa dibuat rekursif atau dengan loop)
                // Untuk kesederhanaan, kita minta pengguna mencoba lagi
                return "REDIRECTED: Silakan coba lagi perintah Anda.";
            }
            return resp;
        } catch (IOException e) {
            System.err.println("Error saat mengirim/menerima data: " + e.getMessage());
            disconnect(); // Tutup koneksi jika ada error
            return "ERROR: " + e.getMessage();
        }
    }

    private void handleRedirection(String response) {
        disconnect(); // Tutup koneksi lama
        String[] parts = response.split(" ");
        if (parts.length > 1 && !parts[1].equalsIgnoreCase("UNKNOWN")) {
            String[] leaderInfo = parts[1].split(":");
            if (leaderInfo.length == 2) {
                this.currentLeaderHost = leaderInfo[0];
                this.currentLeaderPort = Integer.parseInt(leaderInfo[1]);
                System.out.printf("Leader baru diinformasikan: %s:%d. Mencoba menghubungkan kembali...%n", currentLeaderHost, currentLeaderPort);
                // connect(); // Bisa langsung connect atau biarkan sendCommand berikutnya yang handle
            }
        } else {
            System.err.println("Tidak bisa mendapatkan alamat leader baru dari respons.");
        }
    }

    public String ping() {
        return sendCommandAndGetResponse("PING");
    }

    public String get(String key) {
        return sendCommandAndGetResponse("GET " + key);
    }

    public String set(String key, String value) {
        return sendCommandAndGetResponse("SET " + key + " " + value);
    }

    public String append(String key, String value) {
        return sendCommandAndGetResponse("APPEND " + key + " " + value);
    }

    public String del(String key) {
        return sendCommandAndGetResponse("DEL " + key);
    }

    public void disconnect() {
        try {
            if (in != null) in.close();
            if (out != null) out.close();
            if (clientSocket != null) clientSocket.close();
            System.out.println("Koneksi ditutup.");
        } catch (IOException e) {
            System.err.println("Error saat menutup koneksi: " + e.getMessage());
        }
    }
}
