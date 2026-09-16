package client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


public class Client {

    public static void main(String[] args) throws Exception {
        try (Socket socket = new Socket("localhost", 8080);

             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));


        PrintWriter writer =
                new PrintWriter(
                        socket.getOutputStream(),
                        true
                );
        ) {
            writer.println("SUBMIT|task-001|5000");
            String response = reader.readLine();

            System.out.println(
                    "Server: " + response
            );
        }
    }
}
