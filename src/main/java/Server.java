import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
    private static final int PORT = 12345; // Port number for the server

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Server is listening on port " + PORT);

            while (true) {
                Socket socket = serverSocket.accept(); // Accept client connections
                System.out.println("Client connected: " + socket.getInetAddress());

                // Create a new ClientHandler for each client connection
                ClientHandler clientHandler = new ClientHandler(socket);
                // Start a new thread to handle the client
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
