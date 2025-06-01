package com.raft;

import java.util.Scanner;


public class Main {
    public static void main(String[] args) {
        // Alamat dan port node Raft awal yang akan dihubungi
        // Ini harus sesuai dengan salah satu node Raft yang Anda jalankan di Docker
        // dan port yang di-expose ke host.
        String initialRaftHost = "localhost"; // Atau IP Docker host jika App.java di luar
        int initialRaftPort = 8001; // Ganti dengan port yang sesuai

        Scanner scanner = new Scanner(System.in);
        App raftClient = new App(initialRaftHost, initialRaftPort);

        System.out.println("Klien Raft Sederhana. Ketik 'quit' untuk keluar.");
        System.out.println("Perintah yang tersedia: connect, ping, get <key>, set <key> <value>, append <key> <value>, del <key>, disconnect");

        String line;
        while (true) {
            System.out.print("> ");
            line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("quit")) {
                raftClient.disconnect();
                break;
            }

            String[] parts = line.split("\\s+", 3);
            String command = parts[0].toLowerCase();
            String response = "Perintah tidak valid atau error.";

            switch (command) {
                case "connect": // Perintah 'connect' sekarang eksplisit
                    if (raftClient.connect()) {
                        response = "Berhasil terhubung.";
                    } else {
                        response = "Gagal terhubung.";
                    }
                    break;
                case "ping":
                    response = raftClient.ping();
                    break;
                case "get":
                    if (parts.length > 1) response = raftClient.get(parts[1]);
                    else response = "Penggunaan: get <key>";
                    break;
                case "set":
                    if (parts.length > 2) response = raftClient.set(parts[1], parts[2]);
                    else response = "Penggunaan: set <key> <value>";
                    break;
                case "append":
                    if (parts.length > 2) response = raftClient.append(parts[1], parts[2]);
                    else response = "Penggunaan: append <key> <value>";
                    break;
                case "del":
                    if (parts.length > 1) response = raftClient.del(parts[1]);
                    else response = "Penggunaan: del <key>";
                    break;
                case "disconnect": // Perintah 'disconnect' eksplisit
                    raftClient.disconnect();
                    response = "Koneksi telah ditutup.";
                    break;
                default:
                    response = "Perintah tidak dikenal: " + command;
            }
            System.out.println("Server: " + response);
        }
        scanner.close();
        System.out.println("Aplikasi klien ditutup.");
    }
}