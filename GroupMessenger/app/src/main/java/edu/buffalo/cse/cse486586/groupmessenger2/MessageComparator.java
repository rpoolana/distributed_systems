package edu.buffalo.cse.cse486586.groupmessenger2;

import java.util.Comparator;

/**
 * Created by swamy on 3/24/17.
 */

public class MessageComparator implements Comparator<MessageData> {

    @Override
    public int compare(MessageData lhs, MessageData rhs) {

        if(lhs.getSeqNum() < rhs.getSeqNum()) {
            return -1;
        } else if (lhs.getSeqNum() == rhs.getSeqNum()) {
            if(Integer.valueOf(lhs.getProcessId()) < Integer.valueOf(rhs.getProcessId())) {
                return -1;
            } else if(Integer.valueOf(lhs.getProcessId()) > Integer.valueOf(rhs.getProcessId())) {
                return +1;
            }
                return 0;

        } else {
            return +1;
        }
    }
}
