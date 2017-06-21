package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by swamy on 5/3/17.
 */

public class DynamoMessage {

    private Long version;
    private String message;
    private Boolean replicated;
    private String ownerPort;

    public DynamoMessage(String message, boolean replicated, String ownerPort, Long version) {
        this.message = message;
        this.replicated = replicated;
        this.ownerPort = ownerPort;
        this.version = version;
    }

    public DynamoMessage(String message, String replicated, String ownerPort) {
        this.message = message;
        this.replicated = Boolean.valueOf(replicated);
        this.ownerPort = ownerPort;
        this.version = System.currentTimeMillis();
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Boolean isReplicated() {
        return replicated;
    }

    public void setReplicated(boolean replicated) {
        this.replicated = replicated;
    }

    public String getOwnerPort() {
        return ownerPort;
    }

    public void setOwnerPort(String ownerPort) {
        this.ownerPort = ownerPort;
    }

    @Override
    public String toString() {
        return "Message : "+message+" Version : "+version+" Replicated : "+replicated+" Port : "+ ownerPort;
    }
}
