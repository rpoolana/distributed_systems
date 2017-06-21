package edu.buffalo.cse.cse486586.groupmessenger2;

/**
 * Created by swamy on 3/8/17.
 */

public class MessageData {

    private Integer seqNum = 0;
    private String processId;
    private String message;
    private boolean deliverable;

    public Integer getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(Integer seqNum) {
        this.seqNum = seqNum;
    }

    public String getProcessId() {
        return processId;
    }

    public void setProcessId(String processId) {
        this.processId = processId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public boolean isDeliverable() {
        return deliverable;
    }

    public void setDeliverable(boolean deliverable) {
        this.deliverable = deliverable;
    }

    public MessageData(MessageData other) {
        this.seqNum = other.getSeqNum();
        this.processId = other.getProcessId();
        this.message = other.getMessage();
        this.deliverable = other.isDeliverable();
    }

    public MessageData() {

    }

    @Override
    public String toString() {
        return "Process Id : "+getProcessId()+", Message : "+getMessage()+", Sequence num : "+getSeqNum() +", Deliverable : "+isDeliverable();
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof MessageData){
            MessageData messageData = (MessageData)o;
            return this.message == messageData.message;
        }
        return false;
    }
}
