import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable {
    private final ConcurrentHashMap<String, String> m;
    private final Socket socket;
    
    public ClientHandler(Socket socket, ConcurrentHashMap<String, String> m) {
        this.socket = socket;
        this.m = m;
    }

    @Override
    public void run() {
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {

            while (true) {
                String request = input.readLine();
                if (request == null) {
                    continue;
                }
                
                if ("PING".equals(request)) {
                    socket.getOutputStream().write("+PONG\r\n".getBytes());
                } else if ("ECHO".equalsIgnoreCase(request)) {
                    // Read the next line as it's likely part of the protocol
                    input.readLine(); 
                    String message = input.readLine();
                    
                    if (message != null) {
                        // Send the formatted message with length
                        socket.getOutputStream().write(
                            String.format("$%d\r\n%s\r\n", message.length(), message)
                                .getBytes());
                    } else {
                        // Handle the case where message is null
                        socket.getOutputStream().write("Error: No message received.\r\n".getBytes());
                    }
                } else if ("SET".equalsIgnoreCase(request)) {
                    input.readLine(); 
                    String key = input.readLine();
                    input.readLine(); 
                    String value = input.readLine(); 
                    System.out.println(key);
                    m.put(key, value); 
                    socket.getOutputStream().write("+OK\r\n".getBytes());
                } else if ("GET".equalsIgnoreCase(request)) {
                    input.readLine(); 
                    String key = input.readLine(); 
                    String v = m.get(key);
                    if (v != null) {
                        socket.getOutputStream().write(
                            String.format("$%d\r\n%s\r\n", v.length(), v)
                                .getBytes());
                    } else {
                        socket.getOutputStream().write("-1\r\n".getBytes());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
