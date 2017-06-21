package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.KeyEvent;
import android.view.Menu;
import android.view.View;
import android.view.View.OnKeyListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 */
public class GroupMessengerActivity extends Activity {

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    private String myPort = "";

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

    static final int SERVER_PORT = 10000;

    // Map<String, Integer> sequenceMap = new HashMap<String, Integer>();

    Comparator<MessageData> messageDataComparator = new MessageComparator();
    Queue<MessageData> messageQueue = new PriorityBlockingQueue<MessageData>(25,messageDataComparator);
    Map<String, Integer> processCount = new LinkedHashMap<String, Integer>(5);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d("GroupMessenger", "Port : " + myPort);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button b4 = (Button) findViewById(R.id.button4);

        editText.setOnKeyListener(new OnKeyListener() {
            @Override
            public boolean onKey(View v, int keyCode, KeyEvent event) {
                if ((event.getAction() == KeyEvent.ACTION_DOWN) &&
                        (keyCode == KeyEvent.KEYCODE_ENTER)) {

                    String msg = editText.getText().toString() + "\n";
                    editText.setText(""); // This is one way to reset the input box.
                    TextView localTextView = (TextView) findViewById(R.id.local_text_display);
                    localTextView.append("\t" + msg); // This is one way to display a string.
                    TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                    remoteTextView.append("\n");

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
                    return true;
                }
                return false;
            }
        });

        editText.setTextIsSelectable(true);
        editText.setFocusable(true);
        editText.setFocusableInTouchMode(true);
        b4.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.
                TextView localTextView = (TextView) findViewById(R.id.local_text_display);
                localTextView.append("\t" + msg); // This is one way to display a string.
                TextView remoteTextView = (TextView) findViewById(R.id.remote_text_display);
                remoteTextView.append("\n");

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, myPort);
            }
        });

        processCount.put(REMOTE_PORT0,0);
        processCount.put(REMOTE_PORT1,0);
        processCount.put(REMOTE_PORT2,0);
        processCount.put(REMOTE_PORT3,0);
        processCount.put(REMOTE_PORT4,0);

        // new Thread(new MessageDeliveryThread()).start();

    }

    /*private class MessageDeliveryThread implements Runnable {

        @Override
        public void run() {

            while(true) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                MessageData toRemove = null;
                for(MessageData messageData: messageQueue) {
                    if(processCount.get(REMOTE_PORT0) < 5) {
                        if(messageData.getProcessId().equalsIgnoreCase(REMOTE_PORT0) && messageData.isDeliverable()) {
                            sendMessageToContentProvider(messageData);
                            Log.d("Queue", "Sending "+messageData+" to map");
                            processCount.put(REMOTE_PORT0, processCount.get(REMOTE_PORT0)+1);
                            toRemove = messageData;
                        }
                    } else if(processCount.get(REMOTE_PORT1) < 5) {
                        if(messageData.getProcessId().equalsIgnoreCase(REMOTE_PORT1) && messageData.isDeliverable()) {
                            sendMessageToContentProvider(messageData);
                            Log.d("Queue", "Sending "+messageData+" to map");
                            processCount.put(REMOTE_PORT1, processCount.get(REMOTE_PORT1)+1);
                            toRemove = messageData;
                        }
                    } else if(processCount.get(REMOTE_PORT2) < 5) {
                        if(messageData.getProcessId().equalsIgnoreCase(REMOTE_PORT2) && messageData.isDeliverable()) {
                            sendMessageToContentProvider(messageData);
                            Log.d("Queue", "Sending "+messageData+" to map");
                            processCount.put(REMOTE_PORT2, processCount.get(REMOTE_PORT2)+1);
                            toRemove = messageData;
                        }
                    } else if(processCount.get(REMOTE_PORT3) < 5) {
                        if(messageData.getProcessId().equalsIgnoreCase(REMOTE_PORT3) && messageData.isDeliverable()) {
                            sendMessageToContentProvider(messageData);
                            Log.d("Queue", "Sending "+messageData+" to map");
                            processCount.put(REMOTE_PORT3, processCount.get(REMOTE_PORT3)+1);
                            toRemove = messageData;
                        }
                    } else if(processCount.get(REMOTE_PORT4) < 5) {
                        if(messageData.getProcessId().equalsIgnoreCase(REMOTE_PORT4) && messageData.isDeliverable()) {
                            sendMessageToContentProvider(messageData);
                            Log.d("Queue", "Sending "+messageData+" to map");
                            processCount.put(REMOTE_PORT4, processCount.get(REMOTE_PORT4)+1);
                            toRemove = messageData;
                        }
                    }

                }
                if(messageQueue != null) {
                    messageQueue.remove(toRemove);
                }

                Log.d("Queue", "*********Queue is : "+messageQueue+"***********");
                // Log.d("Queue", "After Reorder : " + messageQueue.toString());
                MessageData peek = messageQueue.peek();
                Log.d("Queue", "Peek : " + peek);
                if(messageQueue.size() > 24) {
                    while (peek != null && peek.isDeliverable()) {
                        MessageData poll = messageQueue.poll();
                        Log.d("Queue", "Poll : " + poll);
                        Log.d("Queue", "marked message : " + poll + " for delivery");
                        sendMessageToContentProvider(poll);
                        Log.d("Queue", "Delivered message " + poll + " to the map");
                    }
                }
            }

        }

        private void sendMessageToContentProvider(MessageData toDeliver) {
            ContentValues keyValueToInsert = new ContentValues();

            if(toDeliver.isDeliverable() == false) {
                Log.d("SendMessageToCP", "Message : "+toDeliver.toString()+" is not deliverable");
                return;
            }

            int seqNum = toDeliver.getSeqNum();
            // int seqNum = MapDAO.getProposedSeqNum();
            Log.d("SendMessageToCP","Sequence number is : "+seqNum);
            keyValueToInsert.put("key", seqNum);
            keyValueToInsert.put("value", toDeliver.getMessage());

            Log.d("SendMessageToCP", "Adding message to DB || "+toDeliver.toString());
            Uri newUri = getContentResolver().insert(mUri, keyValueToInsert);
            Log.d("SendMessageToCP", MapDAO.getInstance().printAllData());
        }
    }*/

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        int counter = 0;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {

                    // Thread.sleep(100);

                    Log.d("Server","Waiting for new messages");
                    Socket s = serverSocket.accept();
                    Log.d("Server", "Connection established");
                    InputStream is = s.getInputStream();
                    OutputStream os = s.getOutputStream();
                    Log.d("Server", "Got input and output streams");
                    InputStreamReader isr = new InputStreamReader(is);
                    PrintWriter pw = new PrintWriter(os,true);
                    BufferedReader br = new BufferedReader(isr);

                    String line = "";
                    String msg = "";

                    while((line = br.readLine())!= null && !line.isEmpty()) {
                        Log.d("Server", "Reading data in loop");
                        msg += line;
                        Log.d("Server", "Read line : "+line);
                        break;
                    }

                    Log.d("Port is : ", s.getPort()+"");
                    Log.d("Server", "Message read is : "+msg);

                    if(!msg.trim().contains("|")) {
                        int proposingSeqNum = MapDAO.getInstance().getProposedSeqNum();
                        String resp = "ACK#"+proposingSeqNum;
                        addMessageWithoutSeqNumToQueue(msg);
                        Log.d("Server","Responded : "+resp);
                        pw.println(resp);
                        Log.d("Server","Responded : "+resp);
                    } else {
                        // msg = msg + line;
                        pw.println("ACK");
                        Log.d("Server","Responded : "+"ACK");

                        // Thread.sleep(10);

                        /*Thread thread = new Thread(new MessageDeliveryThread(msg));
                        thread.start();*/

                        changePriorityAndDeliver(msg);
                        //deliverMessage();

                        String[] msgs = new String[10];
                        msgs[0] = msg;
                        publishProgress(msgs);
                        Log.d("Server", "Publishing the msg : "+msg);
                    }

                    is.close();
                    isr.close();
                    os.close();
                    pw.close();
                    br.close();
                    s.close();
                    Log.d("Server","Socket and streams closed");
                }

                // br.close();
                // serverSocket.close();

            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "IO exception : " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Interrupted Exception : " + e.getMessage());
            }

            return null;
        }

        private void addMessageWithoutSeqNumToQueue(String msg) {
            String[] parts = msg.split("\\*");
            String port = parts[0];
            String message = parts[1];

            MessageData messageData = new MessageData();
            messageData.setProcessId(port);
            messageData.setSeqNum(0);
            messageData.setMessage(message);
            messageData.setDeliverable(false);

            messageQueue.add(messageData);
            Log.d("ServerTask", "Added message : ("+messageData+") to the queue. The queue size is : "+messageQueue.size());
        }

        private void changePriorityAndDeliver(String msg) {

            String[] parts = msg.split("\\|");
            String part0 = parts[0];
            String message = parts[1];
            int seqNum = Integer.valueOf(part0);

            MessageData toRemove = null;

            Log.d("Queue", "Before Reorder : "+messageQueue.toString());

            for(MessageData messageData : messageQueue) {
                if(messageData.getMessage().equalsIgnoreCase(message)) {
                    messageData.setDeliverable(true);
                    messageData.setSeqNum(seqNum);
                    toRemove = messageData;
                    break;
                }
            }

            if(toRemove != null) {
                messageQueue.remove(toRemove);
                messageQueue.offer(toRemove);
            }


            Log.d("Queue", "After Reorder : "+messageQueue.toString());
            MessageData peek = messageQueue.peek();
            Log.d("Queue","Peek : "+toRemove);

            if(messageQueue.size() >= 1) {
                while (messageQueue.peek() != null && messageQueue.peek().isDeliverable()) {
                    MessageData poll = messageQueue.poll();
                    Log.d("Queue", "Poll : " + poll);
                    Log.d("Queue", "marked message : " + poll + " for delivery");
                    sendMessageToContentProvider(poll);
                }
            }

        }

        private void sendMessageToContentProvider(MessageData toDeliver) {
            ContentValues keyValueToInsert = new ContentValues();

            if(toDeliver.isDeliverable() == false) {
                Log.d("SendMessageToCP", "Message : "+toDeliver.toString()+" is not deliverable");
                return;
            }

            int seqNum = toDeliver.getSeqNum();
            // int seqNum = MapDAO.getProposedSeqNum();
            Log.d("SendMessageToCP","Sequence number is : "+seqNum);
            keyValueToInsert.put("key", seqNum);
            keyValueToInsert.put("value", toDeliver.getMessage());

            Log.d("SendMessageToCP", "Adding message to DB || "+toDeliver.toString());
            Uri newUri = getContentResolver().insert(mUri, keyValueToInsert);
            Log.d("SendMessageToCP", MapDAO.getInstance().printAllData());
        }


        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();

            Log.d("OnProgressUpdate", "Message received : "+strReceived);

            String[] parts = new String[2];

            if (strReceived != null && strReceived.contains("|")) {
                parts = strReceived.split("\\|");
            }

            Log.d("OnProgressUpdate", "Process id : "+parts[0]);
            Log.d("OnProgressUpdate","Message is : "+parts[1]);

            /*try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            TextView tView = (TextView) findViewById(R.id.textView1);
            tView.append(parts[1] + "\t\n");
            tView.setTextIsSelectable(true);
            tView.setFocusable(true);
            tView.setFocusableInTouchMode(true);

            String filename = "GroupMessengerOutput";
            String string = parts[1] + "\n";
            FileOutputStream outputStream;
            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(parts[1].getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
            Log.d("ProgressUpdate", "Leaving the method");
            return;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {
                String message = msgs[0];
                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");

                List<String> remotePorts = new ArrayList<String>();
                remotePorts.add(REMOTE_PORT0);
                remotePorts.add(REMOTE_PORT1);
                remotePorts.add(REMOTE_PORT2);
                remotePorts.add(REMOTE_PORT3);
                remotePorts.add(REMOTE_PORT4);

                List<Integer> proposedSeqNums = getProposedSequenceNumbers(remotePorts, message);
                Log.d("ClientTask","Proposed Sequence numbers for message : "+message +" is : "+proposedSeqNums.toString());

                int finalSequenceNum = Collections.max(proposedSeqNums);
                Log.d("ClientTask","Chosen final sequence numbers for message : "+message +" is : "+finalSequenceNum);

                /*Thread.sleep(200);
                while(MapDAO.getInstance().getData().keySet().contains(finalSequenceNum+"")) {
                    Thread.sleep(300);
                    Log.d("ClientTask","Chosen final sequence numbers for message : "+message +" : "+finalSequenceNum+" already exists. Redo");
                    proposedSeqNums = getProposedSequenceNumbers(remotePorts, message);
                    Log.d("ClientTask","||| Redo ||| Proposed Sequence numbers for message : "+message +" is : "+proposedSeqNums.toString());
                    finalSequenceNum = Collections.max(proposedSeqNums);
                    Log.d("ClientTask","||| Redo ||| Chosen final sequence numbers for message : "+message +" is : "+finalSequenceNum);
                }*/

                multicastMessageWithFinalSequenceNumber(remotePorts, finalSequenceNum+"|"+message);

            } catch (UnknownHostException e) {
                Log.e("Client", "UnknownHostException");
            } catch (SocketException e) {
                Log.e("Client", "Socket Exception : " + e.getMessage());
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("Client", "IOException : " + e.getMessage());
            } catch (Exception e) {
                Log.e("Client", "Excpetion : "+e.getMessage());
                e.printStackTrace();
            }

            return null;
        }

        private List<Integer> getProposedSequenceNumbers(List<String> remotePorts, String message) throws SocketException, IOException, InterruptedException{
            List<Integer> proposedSeqNums = new ArrayList<Integer>();

            if(message == null || message.isEmpty()) {
                message = "...";
            }

            List<String> removeablePorts = new ArrayList<String>();
            for (String remotePort : remotePorts) {
                try {
                    Log.d("Client", "About to create server connection 1");
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    socket.setSoTimeout(500);

                    Log.d("Client", "Server socket created 1");
                    OutputStream os = socket.getOutputStream();
                    InputStream is = socket.getInputStream();

                    BufferedReader br = new BufferedReader(new InputStreamReader(is));

                    Log.d("Client", "Got output stream 1");
                    PrintWriter pw = new PrintWriter(os, true);

                    Log.d("Client", "Sending message 1");
                    pw.println(myPort+"*"+message);

                   // Thread.sleep(2000);

                    // ACK 1
                    String ack1 = "";
                    String lineAck1 = "";
                    while((lineAck1=br.readLine())!= null && !lineAck1.isEmpty()) {
                        // Thread.sleep(10);
                        Log.d("Client", "Reading ack1 in loop: "+lineAck1);
                        ack1 += lineAck1;
                        break;
                    }

                    int seqNum = extractSeqNum(ack1);

                    if(seqNum == -5) {
                        Log.d("Client", "Port : "+remotePort+" may have a problem. Did not read the acknowledgement");
                    } else {
                        proposedSeqNums.add(seqNum);
                    }

                    Log.d("Client", "Proposed sequence number is : "+seqNum);
                    os.close();
                    is.close();
                    socket.close();
                } catch (IOException e) {
                    if(e instanceof SocketTimeoutException) {
                        Log.d("Client", "Socket time out exception for port : "+remotePort);
                        removeablePorts.add(remotePort);
                    }
                    e.printStackTrace();
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            if(!removeablePorts.isEmpty()) {
                remotePorts.removeAll(removeablePorts);
            }
            return proposedSeqNums;
        }

        private void multicastMessageWithFinalSequenceNumber(List<String> remotePorts, String msgToSend) throws SocketException, IOException {
            List<String> removeablePorts = new ArrayList<String>();
            for (String remotePort : remotePorts) {
                try {
                    Log.d("Client", "About to create server connection 2");

                    Socket socket2 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    socket2.setSoTimeout(500);

                    Log.d("Client", "Server socket created 2");
                    OutputStream os2 = socket2.getOutputStream();
                    InputStream is2 = socket2.getInputStream();

                    BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));

                    Log.d("Client", "Got output stream 2");
                    PrintWriter pw2 = new PrintWriter(os2, true);

                    // send msg with sequence number
                    // String msgWithPort = myPort+"|"+msgToSend;
                    Log.d("Client", "Writing msg : "+msgToSend);
                    pw2.println(msgToSend);
                    // Log.d("Client", "Wrote the msg : "+msgToSend);

                    //Thread.sleep(100);

                    // ACK 2
                    String ack2 = "";
                    String lineAck2 = "";
                    while((lineAck2=br2.readLine())!= null && !lineAck2.isEmpty()) {
                        Log.d("Client", "Reading ack2 in loop: "+lineAck2);
                        ack2 += lineAck2;
                        break;
                    }
                    Log.d("Client", "Read the msg from the server : "+ack2);
                    os2.close();
                    is2.close();
                    socket2.close();
                    Log.d("Client", "Socket closed");
                } catch (IOException e) {
                    if(e instanceof SocketTimeoutException) {
                        Log.d("Client", "Socket time out exception for port : "+remotePort);
                        removeablePorts.add(remotePort);
                        continue;
                    }
                    e.printStackTrace();
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            if(!removeablePorts.isEmpty()) {
                remotePorts.removeAll(removeablePorts);
            }
        }

        private int extractSeqNum(String proposedSeqNumWithAck) {
            Log.d("Client", "Sequence num with ack : "+proposedSeqNumWithAck);

            int seqNum = 0;
            try {
                String [] parts = proposedSeqNumWithAck.split("\\#");
                seqNum = Integer.valueOf(parts[1]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            } catch (ArrayIndexOutOfBoundsException e) {
                Log.d("extractSeqNum","ArrayIndexOutOfBoundsException"+e.getMessage());
                seqNum = -5;
            }

            Log.d("Client", "Proposed sequence number is : "+seqNum);
            return seqNum;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
