import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Main {
    static class ExpiryAndValue {
        final long expiryTimestamp; // in ms
        final String value;
        ExpiryAndValue(String value) {
            this.expiryTimestamp = Long.MAX_VALUE;
            this.value = value;
        }
        ExpiryAndValue(long expiryTimestamp, String value) {
            this.expiryTimestamp = expiryTimestamp;
            this.value = value;
        }
    }
    static Map<String, ExpiryAndValue> cache = new HashMap<>();
    public static void main(String[] args) throws IOException {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");
        int port = 6379;
        String master_host = null; 
        int master_port = -1;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
                i++; // Skip the next argument as it's the port number
            } else if (args[i].equals("--replicaof") && i + 1 < args.length) {
                // The --replicaof argument should be in quotes
                String replicaInfo = args[i + 1];
                // Split the string to extract host and port
                String[] parts = replicaInfo.split(" ");
                if (parts.length == 2) {
                    master_host = parts[0];
                    master_port = Integer.parseInt(parts[1]);
                    // Send a PING to the master_host and master_port
                    Socket masterSocket = new Socket(master_host, master_port);

                    sendPingToMaster(master_host, master_port);
                    // Read from the input of the master
                    masterSocket.getInputStream().read();
                    sendREPLCONFToMaster(master_host, master_port);  
                    
                }
                i++; // Skip the next argument as it's the replica info
            }
        }
        Selector selector = Selector.open();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try (ServerSocketChannel serverSocket = ServerSocketChannel.open()) {
            serverSocket.bind(new InetSocketAddress("localhost", port));
            serverSocket.configureBlocking(false);
            serverSocket.register(selector, SelectionKey.OP_ACCEPT);
            // Wait for connection from client.
            while (true) {
                selector.select();
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys) {
                    if (key.isAcceptable()) {
                        SocketChannel client = serverSocket.accept();
                        client.configureBlocking(false);
                        client.register(selector, SelectionKey.OP_READ);
                    }
                    if (key.isReadable()) {
                        buffer.clear();
                        SocketChannel client = (SocketChannel) key.channel();
                        int bytesRead = client.read(buffer);
                        if (bytesRead == -1) { //end of stream
                            continue;
                        }
                        buffer.flip();
                        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(buffer);
                        System.out.println("command: " + charBuffer);
                        List<String> parsedCommand = parseCommand(charBuffer);
                        buffer.clear();
                        processCommand(parsedCommand, buffer, master_port, master_host);
                        buffer.flip();
                        client.write(buffer);
                    }
                }
                selectedKeys.clear();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Done!");
        }
    }

    static String bulkString(String str) {
        if (str == null) {
            return "$-1\r\n";
        }
        return "$" + str.length() + "\r\n" + str + "\r\n";
    }

    static void processCommand(List<String> parsedCommand, ByteBuffer buffer, int master_port, String master_host) {
        String cmd = parsedCommand.get(0);
        String response = "+ERROR\n";
        if (cmd.equalsIgnoreCase("PING")) {
            response = "+PONG\r\n";
        } else if (cmd.equalsIgnoreCase("ECHO")) {
            response = "+" + parsedCommand.get(1) + "\r\n";
        } else if (cmd.equalsIgnoreCase("SET")) {
            cache.put(parsedCommand.get(1), new ExpiryAndValue(parsedCommand.get(2)));
            ExpiryAndValue toStore;
            if (parsedCommand.size() > 3 && parsedCommand.get(3).equalsIgnoreCase("PX")) {
                int milis = Integer.parseInt(parsedCommand.get(4));
                long expiryTimestamp = System.currentTimeMillis() + milis;
                toStore = new ExpiryAndValue(expiryTimestamp, parsedCommand.get(2));
            } else {
                toStore = new ExpiryAndValue(parsedCommand.get(2));
            }
            cache.put(parsedCommand.get(1), toStore);
            response = "+OK\r\n";
        } else if (cmd.equalsIgnoreCase("GET")) {
            ExpiryAndValue cached = cache.get(parsedCommand.get(1));
            if (cached != null && cached.expiryTimestamp >= System.currentTimeMillis()) {
                String content = cached.value;
                response = "$" + content.length() + "\r\n" + content + "\r\n";
            } else { // nil
                response = "$-1\r\n";
            }
        } else if (cmd.equalsIgnoreCase("INFO")) {
            String role = "slave";
            if (master_port == -1) {
                role = "master"; 
            } 
            StringBuilder sb = new StringBuilder();
            sb.append("role:").append(role).append("\n");
            sb.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n");
            sb.append("master_repl_offset:0");
            response = bulkString(sb.toString());
                                    
        } 

        buffer.put(response.getBytes(StandardCharsets.UTF_8));
    }

    static List<String> parseCommand(CharBuffer data) {
        Tokenizer tokenizer = new Tokenizer(data);
        tokenizer.chomp('*');
        int arraySize = tokenizer.nextInt();
        tokenizer.chomp('\r');
        tokenizer.chomp('\n');
        if (arraySize == 0) {
            System.out.println("Empty array. Was that intended?");
            return Collections.emptyList();
        }
        List<String> args = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            tokenizer.chomp('$');
            int argLength = tokenizer.nextInt();
            tokenizer.chomp('\r');
            tokenizer.chomp('\n');
            String arg = tokenizer.nextString(argLength);
            tokenizer.chomp('\r');
            tokenizer.chomp('\n');
            args.add(arg);
        }
        System.out.println("args: " + args);
        return args;
    }

    static class Tokenizer {
        final CharBuffer buf;
        Tokenizer(CharBuffer buf) {this.buf = buf.asReadOnlyBuffer();}
        boolean expect(char expected) {
            return current() == expected;
        }
        void chomp(char expected) {
            if (buf.get() != expected) {
                throw new IllegalStateException(buf + "[" + (buf.position() - 1) + "] != " + expected);
            }
        }
        char current() {
            return buf.get(buf.position());
        }
        int nextInt() {
            int start = buf.position();
            char curr = buf.get();
            StringBuilder sb = new StringBuilder();
            while (curr >= '0' && curr <= '9') {
                sb.append(curr);
                curr = buf.get();
            }
            buf.position(buf.position() - 1);
            if (buf.position() == start) {
                throw new IllegalStateException(buf + "[" + start + "] is not an int");
            }
            return Integer.parseInt(sb.toString());
        }
        String nextString(int len) {
            if (buf.remaining() < len) {
                throw new IllegalStateException("out of bounds read");
            }
            char[] chars = new char[len];
            buf.get(chars, 0, len);
            return new String(chars);
        }
    }

    static void sendPingToMaster(String masterHost, int masterPort) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(masterHost, masterPort), 5000); // 5 second timeout
            OutputStream out = socket.getOutputStream();
            out.write("*1\r\n$4\r\nping\r\n".getBytes(StandardCharsets.UTF_8));
            out.flush();
            System.out.println("PING sent to " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            System.err.println("Failed to send PING to " + masterHost + ":" + masterPort);
            e.printStackTrace();
        }
    }  

    static void sendREPLCONFToMaster(String masterHost, int masterPort) { 
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(masterHost, masterPort), 5000); // 5 second timeout
            OutputStream out = socket.getOutputStream();

            // Use String.format to insert the masterPort value
            // TODO this could be make the coder better by using bulk string. I could rewrite it. 
            String replconf = String.format("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n", 
                                            String.valueOf(masterPort).length(), masterPort);
            out.write(replconf.getBytes(StandardCharsets.UTF_8));
            // String replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            // out.write(replconf2.getBytes(StandardCharsets.UTF_8));
            out.flush();
            System.out.println("REPLCONF sent to " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            System.err.println("Failed to send REPLCONF to " + masterHost + ":" + masterPort);
            e.printStackTrace();
        }
    }
}