import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable {
    private final ConcurrentHashMap<String, Main.Value> m;
    private final Socket socket;
    
    public ClientHandler(Socket socket, ConcurrentHashMap<String, Main.Value> m) {
        this.socket = socket;
        this.m = m;
    }

    @Override
    public void run() {
        try (BufferedReader input = new BufferedReader(
                new InputStreamReader(socket.getInputStream()))) {
            int numberOfCommands = 0; 
            while (true) {
                String request = input.readLine(); 
                
                if (request == null) {
                    continue;
                } 
                // System.out.println(request);
                if (request.contains("*")) {
                    try {
                        // Extract the part after the '*'
                        String numberString = request.substring(1);
                        // Parse this substring to an integer
                        numberOfCommands = Integer.parseInt(numberString);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number of commands: " + request);
                        continue;
                    }
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
                    // This readLine does ntow ork becaues, it is not null, it does not have any input any
                    if (numberOfCommands == 5) {
                        input.readLine(); 
                        // Put the expery date in this, and this will be fine. 
                        String cmd = input.readLine(); // This is expectd to be px
                        if ("px".equalsIgnoreCase(cmd)) {
                            input.readLine();   
                            String expiry = input.readLine();  
                            long expiryTime = System.currentTimeMillis() + Long.parseLong(expiry);
                            m.put(key, new Main.Value(value, expiryTime)); 
                        }  
                    } else {
                        m.put(key, new Main.Value(value, Long.MAX_VALUE)); 
                    }
                    // It can be the time, 
                    // 1. Check if there is pk, and check if there is time period, when add to the map, also add the end time
                    socket.getOutputStream().write("+OK\r\n".getBytes());
                } else if ("GET".equalsIgnoreCase(request)) {
                    input.readLine(); 
                    String key = input.readLine(); 
                    Main.Value v = m.get(key);     
                    if (v != null) {
                        if (v.isExpired()) {
                            socket.getOutputStream().write("$-1\r\n".getBytes());
                            
                        } else {
                            System.out.print("current time");
                            System.out.println(System.currentTimeMillis());
                            System.out.println(v.expiryTime);
                            socket.getOutputStream().write(
                            String.format("$%d\r\n%s\r\n", v.data.length(), v.data)
                                .getBytes());
                        }
                    } else {
                        socket.getOutputStream().write("$-1\r\n".getBytes());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
