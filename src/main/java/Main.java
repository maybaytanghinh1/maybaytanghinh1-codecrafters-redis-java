import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
import java.util.Base64;
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
                    try (Socket masterSocket = new Socket(master_host, master_port)) {
                        PrintWriter out = new PrintWriter(masterSocket.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(masterSocket.getInputStream()));

                        // Send a PING to the masterHost and masterPort using the same socket
                        out.print("*1\r\n$4\r\nping\r\n");
                        out.flush();
                        String response = in.readLine();
                        if (!response.equals("+PONG")) {
                            throw new IOException("Did not receive PONG from master");
                        }

                        out.print(
                            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n");
                        out.flush();
                        response = in.readLine();
                        if (!response.equals("+OK")) {
                            throw new IOException("Did not receive OK from master after REPLCONF listening-port");
                        }

                        out.print("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
                        out.flush();
                        response = in.readLine();
                        if (!response.equals("+OK")) {
                            throw new IOException("Did not receive OK from master after REPLCONF capa psync2");
                        }
                        
                        // Send PSYNC command
                        out.print("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"); 
                        out.flush();
                        response = in.readLine();
                        if (!response.startsWith("+OK")) {
                            throw new IOException("Did not receive OK from master after PSYNC");
                        }
                    } catch (IOException e) {
                        System.err.println("Failed to connect to the master at " + master_host + ":" + master_port);
                        e.printStackTrace();
                    }
                    
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
        Boolean isPsync = false;
        
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
                                    
        } else if (cmd.equalsIgnoreCase("REPLCONF")) {
            response = "+OK\r\n";
        } else if (cmd.equalsIgnoreCase("PSYNC")) {
            // Assume that this is always -1 
            // I hardcoded the string here. 
            response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
            isPsync = true; 
            // I want to send an empty RDB file in binary.  
        }

        
        buffer.put(response.getBytes(StandardCharsets.UTF_8));

        String emptyRDB = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        StringBuilder binaryRepresentation = new StringBuilder();
        byte[] decodedBytes = Base64.getDecoder().decode(emptyRDB);
        for (byte b : decodedBytes) {
            binaryRepresentation.append(String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0'));
        }
        
        // Print the binary representation
        binaryRepresentation.toString();
        String outputString = "$" + binaryRepresentation.length() + "\r\n" + binaryRepresentation.toString();

        if (isPsync) {
            buffer.put(outputString.getBytes(StandardCharsets.UTF_8));
        }
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
}