import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static class Value {
        String data;
        long expiryTime;

        Value(String data, long expiryTime) {
          this.data = data;
          this.expiryTime = expiryTime;
        }

        boolean isExpired() {
          return expiryTime > 0 && System.currentTimeMillis() > expiryTime;
        }
    }
    
    private static final int PORT = 6379; // Port number for the server
    public static ConcurrentHashMap<String, Value> m = new ConcurrentHashMap<>();
    // New for ConcurrentHashMap 

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server is listening on port " + PORT);
            while (true) {
                Socket socket = serverSocket.accept(); // Accept client connections
                System.out.println("Client connected: " + socket.getInetAddress());

                // Create a new ClientHandler for each client connection
                ClientHandler clientHandler = new ClientHandler(socket, m);
                // Start a new thread to handle the client
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
