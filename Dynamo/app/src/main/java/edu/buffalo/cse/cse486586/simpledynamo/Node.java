package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

public class Node implements Comparable<Node> {
    private Node successor;
    private Node predecessor;
    private boolean active;

    private String port;
    private String hash;

    public Node(String port) throws NoSuchAlgorithmException {
        this.port = port;
        this.hash = genHash(port);
        this.active = true;
    }

    @Override
    public int compareTo(Node other) {
        if (this.hash.compareTo(other.getHash()) < 0) {
            return -1;
        } else if (this.hash.compareTo(other.getHash()) == 0) {
            return 0;
        } else {
            return 1;
        }
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Node getSuccessor() {
        return successor;
    }

    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    public Node getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
    }

    public String getPort() {
        return Integer.valueOf(this.port)*2+"";
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    @Override
    public String toString() {
        if (successor == null || predecessor == null) {
            return "@@Port : " + port + " hash : " + hash+" Status : "+isActive();
        } else {
            return "@@Port : " + port + " Hash : " + hash+" Status : "+isActive() + " Predecessor : " + predecessor.port + " Successor : " + successor.port;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


}
