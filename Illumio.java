import java.io.*;
import java.util.*;

public class Illumio {
    public static HashMap<String, String> lookup = new HashMap<>();
    public static HashMap<String, Integer> tags = new HashMap<>();
    public static HashMap<String, Integer> protocols = new HashMap<>();

    public static void main(String[] args) throws IOException {
        lookUpFn("lookup.csv");
        parseLogFn("flowLogs.txt");
        createTag("tagCount.csv");
        createProtocol("protocolCount.csv");
    }

    public static void lookUpFn(String file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            reader.readLine();
            String curr;
            while ((curr = reader.readLine()) != null) {
                String[] parts = curr.split(",");
                lookup.put(parts[0] + "," + parts[1].toLowerCase(), parts[2]);
            }
        }
    }

    public static void parseLogFn(String file) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String curr;
            while ((curr = reader.readLine()) != null) {
                String[] parts = curr.split("\\s+");
                String dst = parts[6];
                String protocol = parts[7].equals("6") ? "tcp" : parts[7].equals("17") ? "udp" : "unknown";
                String tag = lookup.getOrDefault(dst + "," + protocol, "Untagged");
                tags.put(tag, tags.getOrDefault(tag, 0) + 1);
                String pair = dst + "," + protocol;
                protocols.put(pair, protocols.getOrDefault(pair, 0) + 1);
            }
        }
    }

    public static void createTag(String file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Tag,Count");
            for (String k : tags.keySet())
                writer.printf("%s,%d%n", k, tags.get(k));
        }
    }

    public static void createProtocol(String file) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(file))) {
            writer.println("Port,Protocol,Count");
            for (String k : protocols.keySet()) {
                String[] parts = k.split(",");
                writer.printf("%s,%s,%d%n", parts[0], parts[1], protocols.get(k));
            }
        }
    }
}
