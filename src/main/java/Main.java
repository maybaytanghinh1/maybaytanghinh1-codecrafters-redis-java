import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HexFormat;

public class Main {
    public static int port = 6379;
    public static String role = "master";
    // replica-only
    public static String master_host;
    public static int master_port;
    // master-only
    public static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    public static long master_repl_offset = 0;

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
            new Thread(new ClientHandler(socket)).start();
        }

        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
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

    public ClientHandler(Socket socket) {
        this.socket = socket;
        try {
            // Wait for 1 second (1000 milliseconds)
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // Handle the exception if the thread is interrupted while sleeping
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            os = socket.getOutputStream();
            String line;
            List<String> command = new ArrayList<>();
            int commandLines = 0;

            while ((line = in.readLine()) != null) {
                System.out.println("line: " + line);
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
                String cmd = lines[2].toLowerCase();

                if (cmd.equals("ping")) {
                    write("+PONG\r\n");
                } else if (cmd.equals("replconf")) {
                    write("+OK\r\n");
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
                    System.out.println("SET Key and Value: " + key + " = " + value);
                    map.put(key, value);
                    if (lines.length == 11 && lines[8].equalsIgnoreCase("px")) {
                        int millis = Integer.parseInt(lines[10]);
                        expiry.put(key, System.currentTimeMillis() + millis);
                    }
                    write("+OK\r\n");
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
                    List<String> kvps = new ArrayList<>();
                    kvps.add("role:" + Main.role);
                    if (Main.role.equals("master")) {
                        kvps.add("master_replid:" + Main.master_replid);
                        kvps.add("master_repl_offset:" + Main.master_repl_offset);
                    }
                    write(bulkString(kvps));
                }
            }
        }
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
