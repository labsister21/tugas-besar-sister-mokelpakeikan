package com.raft;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        App raftClient = null;

        System.out.println("Klien Raft Sederhana. Ketik 'quit' untuk keluar.");
        System.out.println("Perintah yang tersedia: connect <host> <port>, ping, get <key>, set <key> <value>, append <key> <value>, del <key>, disconnect");

        String line;
        while (true) {
            System.out.print("> ");
            line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("quit")) {
                if (raftClient != null) {
                    raftClient.disconnect();
                }
                break;
            }

            String[] parts = line.split("\\s+", 3);
            String command = parts[0].toLowerCase();
            String response = "Perintah tidak valid atau error.";

            switch (command) {
                case "connect":
                    if (parts.length == 3) {
                        String host = parts[1];
                        try {
                            int port = Integer.parseInt(parts[2]);
                            raftClient = new App(host, port);
                            if (raftClient.connect()) {
                                response = "Berhasil terhubung ke " + host + ":" + port;
                            } else {
                                response = "Gagal terhubung ke " + host + ":" + port;
                            }
                        } catch (NumberFormatException e) {
                            response = "Port harus berupa angka";
                        }
                    } else {
                        response = "Penggunaan: connect <host> <port>";
                    }
                    break;
                case "add-server":
                    if (raftClient != null) {
                        if (parts.length == 3) {
                            String serverHost = parts[1];
                            try {
                                int serverPort = Integer.parseInt(parts[2]);
                                response = raftClient.addServer(serverHost, serverPort);
                            } catch (NumberFormatException e) {
                                response = "Port harus berupa angka";
                            }
                        } else {
                            response = "Penggunaan: add-server <host> <port>";
                        }
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                    
                case "remove-server":
                    if (raftClient != null) {
                        if (parts.length == 3) {
                            String serverHost = parts[1];
                            try {
                                int serverPort = Integer.parseInt(parts[2]);
                                response = raftClient.removeServer(serverHost, serverPort);
                            } catch (NumberFormatException e) {
                                response = "Port harus berupa angka";
                            }
                        } else {
                            response = "Penggunaan: remove-server <host> <port>";
                        }
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "ping":
                    if (raftClient != null) {
                        response = raftClient.ping();
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "get":
                    if (raftClient != null) {
                        if (parts.length > 1) response = raftClient.get(parts[1]);
                        else response = "Penggunaan: get <key>";
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "set":
                    if (raftClient != null) {
                        if (parts.length > 2) response = raftClient.set(parts[1], parts[2]);
                        else response = "Penggunaan: set <key> <value>";
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "append":
                    if (raftClient != null) {
                        if (parts.length > 2) response = raftClient.append(parts[1], parts[2]);
                        else response = "Penggunaan: append <key> <value>";
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "del":
                    if (raftClient != null) {
                        if (parts.length > 1) response = raftClient.del(parts[1]);
                        else response = "Penggunaan: del <key>";
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "strln":
                    if (raftClient != null) {
                        if (parts.length > 1) response = raftClient.strln(parts[1]);
                        else response = "Penggunaan: strln <key>";
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "getlog":
                    if (raftClient != null) {
                        response = raftClient.getLog();
                    } else {
                        response = "Belum terhubung ke server. Gunakan 'connect <host> <port>' terlebih dahulu.";
                    }
                    break;
                case "disconnect":
                    if (raftClient != null) {
                        raftClient.disconnect();
                        response = "Koneksi telah ditutup.";
                    } else {
                        response = "Tidak ada koneksi aktif.";
                    }
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