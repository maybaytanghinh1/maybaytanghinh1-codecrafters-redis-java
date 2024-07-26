import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import javax.management.openmbean.OpenDataException;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");
        
        int port = 6379;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            
            while (true) {
                try {
                    // Wait for connection from client.
                    Socket clientSocket = serverSocket.accept();
                    
                    // Handle each client connection in a new thread
                    new Thread(() -> handlePing(clientSocket)).start();
                } catch (IOException e) {
                    System.err.println("Accept failed: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port: " + port);
            System.exit(-1);
        }
    }

    public static void handlePing(Socket clientSocket) {
        // Uncomment this block to pass the first stage
        try (
            // Set up input and output streams for the client.
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            // Read input from the client.
            String clientMessage;
            while ((clientMessage = in.readLine()) != null) {
                if (clientMessage.equals("PING")) {
                    out.println("+PONG\r");
                }   
                //Check if it echo. 
                if ("ECHO".equalsIgnoreCase(clientMessage)) {
                    in.readLine();
                    String message = in.readLine(); 
                    out.print(String.format("$%d\r\n%s\r\n", message.length(), message));
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("IOException on closing client socket: " + e.getMessage());
            }
        }
    }
}
