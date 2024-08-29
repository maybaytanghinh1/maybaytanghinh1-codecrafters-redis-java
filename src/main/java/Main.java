import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;

public class Main {
    public static int port = 6379;
    public static String role = "master";
    // replica-only
    public static String master_host;
    public static int master_port;
    // master-only
    public static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    public static long master_repl_offset = 0;
    public static List<OutputStream> replicas = new ArrayList<>();
    public static int offset = 0; 
    public static String dir = ""; 
    public static String fileName = "";

    public static void main(String[] args) throws IOException {
        System.out.println("Log will happen here");
        for (String arg : args) {
            System.out.println(arg);
        }
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
    
        // ./spawn_redis_server.sh --port <PORT>
        if (args.length >= 2 && args[0].equalsIgnoreCase("--port")) {
            port = Integer.parseInt(args[1]);
        }
        // ./spawn_redis_server.sh --port <PORT> --replicaof <MASTER_HOST> <MASTER_PORT>
        if (args.length >= 4 && args[2].equalsIgnoreCase("--replicaof")) {
            String replicaInfo = args[3];
            String[] parts = replicaInfo.split(" ");
            role = "slave";
            master_host = parts[0];
            master_port = Integer.parseInt(parts[1]);
            Socket socket = new Socket(master_host, master_port);
            OutputStream os = socket.getOutputStream();
            InputStream is = socket.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(is));
            String line;
            os.write("*1\r\n$4\r\nping\r\n".getBytes());
            System.out.println("line=" + in.readLine());
            // REPLCONF listening-port <PORT>
            os.write(("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" +
                      port + "\r\n").getBytes());
            System.out.println("line=" + in.readLine());
            // REPLCONF capa psync2
            os.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".getBytes());
            System.out.println("line=" + in.readLine());
            // PSYNC ? -1
            os.write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".getBytes());
            System.out.println("line=" + in.readLine());
            // Minh tuong no hadnel roi, maybe test acse cuar minh
            os.write("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n".getBytes());
            new Thread(new ClientHandler(socket, role)).start();
        } 
        
        if (args.length >= 4 && args[0].equalsIgnoreCase("--dir")) {
            dir = args[1];  
            fileName = args[3]; 
        }


        try {
            // System.out.println("Get in here 2");
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket, role)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}


class ClientHandler implements Runnable {
    private Socket socket;
    private OutputStream os; 
    // I need to have an array of replica and that it. 
    private static final Map<String, String> map = new HashMap<>();
    private static final Map<String, Long> expiry = new HashMap<>();
    private static final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    public String role; 

    public ClientHandler(Socket socket, String role) {
        this.socket = socket;
        this.role = role; 
        try {
            // Wait for 1 second (1000 milliseconds)
            // TODO I do not know if I have to sleep here. It seems wrong to me for some reason. 
            // I am not 100% sure. I finally figure this out fo rnwo. 
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // Handle the exception if the thread is interrupted while sleeping
            e.printStackTrace();
        }

    }
    
    @Override
    public void run() {
        System.out.println("Get to the run");
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream())); 

            os = socket.getOutputStream();
            String line;
            List<String> command = new ArrayList<>();
            int commandLines = 0;
            int bytesRead = 0; // Variable to track bytes read for each line
            Main.offset += bytesRead;
            while ((line = in.readLine()) != null) {
                // System.out.println("line in run: " + line);
                bytesRead = line.length() + 2; // \r\n adds 2 bytes
                Main.offset += bytesRead;
                if (commandLines == 0) {
                    commandLines = 1 + 2 * Integer.parseInt(line.substring(1));
                    command.add(line);
                } else if (command.size() < commandLines) {
                    command.add(line);
                }
                if (command.size() == commandLines) {
                    queue.put(String.join("\r\n", command)); // Place the command into the queue
                    commandLines = 0;
                    command.clear();
                }
                // Process commands from the queue
                processCommands();
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Exception: " + e.getMessage());
        }
    }

    private void processCommands() throws IOException {
        while (!queue.isEmpty()) {
            String command = queue.poll();
            if (command != null) {
                String[] lines = command.split("\r\n");
                System.out.println("Ge to the commands");
                String cmd = lines[2].toLowerCase();
                System.out.println(cmd);
                if (cmd.equals("ping")) {
                    Main.replicas.add(os);
                    System.out.println("Master is here");
                    if (role == "master") {
                        write("+PONG\r\n");
                    }
                } else if (cmd.equals("replconf")) {
                    for (String tmp : lines) {
                        System.out.println(tmp);
                    }
                    // TODO there should be a way to store this
                    System.out.println(lines[3]);
                    if (lines[4].equals("GETACK")) {
                        os.write(String.format("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%s\r\n%d\r\n", Integer.toString(Main.offset).length(), Main.offset).getBytes());
                    } else {
                        write("+OK\r\n");
                    }
                    
                } else if (cmd.equals("psync")) {
                    write("+FULLRESYNC %s 0\r\n".formatted(Main.master_replid));
                    byte[] contents = HexFormat.of().parseHex(
                        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");
                    write("$" + contents.length + "\r\n");
                    os.write(contents);
                } else if (cmd.equals("echo")) {
                    String value = lines[4];
                    write(bulkString(value));
                } else if (cmd.equals("set")) {
                    String key = lines[4];
                    String value = lines[6];
                    command = String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",key.length(), key, value.length(), value);
                    if (role == "master") {
                        for (OutputStream os_slave : Main.replicas) {
                            os_slave.write(command.getBytes());
                        }
                    }
                    System.out.println("SET Key and Value: " + key + " = " + value);
                    map.put(key, value);
                    if (lines.length == 11 && lines[8].equalsIgnoreCase("px")) {
                        int millis = Integer.parseInt(lines[10]);
                        expiry.put(key, System.currentTimeMillis() + millis);
                    }
                    if (role == "master") {
                        write("+OK\r\n");
                    }
                } else if (cmd.equals("get")) {
                    String key = lines[4];
                    String value = map.get(key);
                    System.out.println("GET Key: " + key);
                    boolean expired = false;
                    if (expiry.containsKey(key)) {
                        if (System.currentTimeMillis() > expiry.get(key)) {
                            expired = true;
                            map.remove(key);
                            expiry.remove(key);
                        }
                    }
                    if (expired) {
                        write("$-1\r\n");
                    } else {
                        write(bulkString(value));
                    }
                } else if (cmd.equals("info")) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("role:").append(role).append("\n");
                    sb.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n");
                    sb.append("master_repl_offset:0");
                    String str = sb.toString();
                    write("$" + str.length() + "\r\n" + str + "\r\n");
                } else if (cmd.equals("wait")) {
                    // TODO how does it know the ack of the replicas, they might have to send the ack 
                    // There will be time out. 
                    // If there is no write commands, then the wait should return nothonig 
                    write(String.format(":%d\r\n", Main.replicas.size()));
                } else if (cmd.equals("config")) {
                    System.out.println(lines[6].toLowerCase());
                    if (lines[6].toLowerCase().equals("dir")) {
                        write(String.format(
                            "*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", Main.dir.length(), Main.dir
                        ));
                    } else {
                        write(bulkString(Main.fileName)); 
                    }
                } else if (cmd.equals("keys")) {
                    InputStream inputStream = new FileInputStream(new File(Main.dir, Main.fileName));   
                    byte[] redis = new byte[5];
                    byte[] version = new byte[4];
                    inputStream.read(redis);
                    inputStream.read(version);
                    System.out.println("Magic String = " +
                                        new String(redis, StandardCharsets.UTF_8));
                    System.out.println("Version = " +
                                        new String(version, StandardCharsets.UTF_8));  
                    int read;
                    while ((read = inputStream.read()) != -1) {
                        if (read == 0xFB) {
                            getLen(inputStream);
                            getLen(inputStream);
                            break;
                        }
                        }
                        int type = inputStream.read();
                        int len = getLen(inputStream);
                        byte[] key_bytes = new byte[len];
                        inputStream.read(key_bytes);
                        String parsed_key = new String(key_bytes);
                        write("*1\r\n$" + parsed_key.length() + "\r\n" +
                                        parsed_key + "\r\n");
                }
            }
        }
    }

    private static int getLen(InputStream inputStream) throws IOException {
        int read;
        read = inputStream.read();
        int len_encoding_bit = (read & 0b11000000) >> 6;
        int len = 0;
        if (len_encoding_bit == 0) {
            len = read & 0b00111111;
        } else if (len_encoding_bit == 1) {
            int extra_len = inputStream.read();
            len = ((read & 0b00111111) << 8) + extra_len;
        } else if (len_encoding_bit == 2) {
            byte[] extra_len = new byte[4];
            inputStream.read(extra_len);
            len = ByteBuffer.wrap(extra_len).getInt();
        }
        return len;
    }

    private String bulkString(String value) {
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    private String bulkString(List<String> values) {
        if (values.size() == 1) {
            return bulkString(values.get(0));
        }
        StringBuilder result = new StringBuilder("*" + values.size() + "\r\n");
        for (String value : values) {
            result.append(bulkString(value));
        }
        return result.toString();
    }

    private void write(String s) throws IOException {
        os.write(s.getBytes());
    }
}
