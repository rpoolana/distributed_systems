package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static android.R.attr.value;
import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

    private String predecessor;
    private String successor;
    private String nodeId;
    private String myPort;
    private boolean isMaster;
    private static String MASTER_NODE = "11108";
    private Map<String, String> activeNodes = new TreeMap<String, String>();
    private Map<String, String> redistributingData = new TreeMap<String, String>();
    private Map<String, String> qAllData = new HashMap<String,String>();
    private Map<String, String> qOneData = new HashMap<String, String>();

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
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

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d("CP_create", "My Port is : " + myPort);

        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        try {
            activeNodes.put(genHash(Long.valueOf(myPort)/2+""), myPort);
            Log.d("CP_create", "Added node : " + genHash(Long.valueOf(myPort)/2+"") + " for port num : " + myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (myPort.equalsIgnoreCase(MASTER_NODE)) {
            isMaster = true;
            Log.d("CP_create", myPort + " is master node");
        }

        Log.d("CP_create", "Am I master node? : " + isMaster);

        try {
            Log.d("CP_create", "Generating node id for : " + myPort);
            nodeId = genHash(Long.valueOf(myPort)/2+"");
            Log.d("CP_create", "Node Id ; " + nodeId);
        } catch (NoSuchAlgorithmException e) {
            Log.d("CP_create", "Hash function failed : " + e.getMessage());
        }

        if (!isMaster) {
            // send node join request to master
            final String[] msgs = new String[2];
            msgs[0] = "NODE_JOIN";
            msgs[1] = MASTER_NODE;

            /*try {
                Thread.sleep(4000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgs[0], msgs[1]);
        }
        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        try {
            String key = values.get("key").toString();
            String value = (String) values.get("value");
            String keyHash = genHash(key);

            Log.d("CP_insert", "Values to insert : key " + key + " .... value " + value);

            String minHashKey = Collections.min(activeNodes.keySet());
            int lessThanMin = keyHash.compareTo(minHashKey);
            Log.d("CP_insert","Less than min = "+ lessThanMin);

            String maxHashKey = Collections.max(activeNodes.keySet());
            int moreThanMax = keyHash.compareTo(maxHashKey);
            Log.d("CP_insert", "More than max = "+moreThanMax);

            if (lessThanMin <= 0 || moreThanMax > 0) {
                Log.d("CP_insert", "Insert to min hash node");
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + value, activeNodes.get(minHashKey)).get();
                return uri;
            } else {
                Log.d("CP_insert", "Global insert");
                Set<String> keySet = activeNodes.keySet();
                Iterator<String> it = keySet.iterator();

                while (it.hasNext()) {
                    String currentNode = it.next();
                    int compare = keyHash.compareTo(currentNode);
                    Log.d("CP_insert", "Comparing the key : " + keyHash + " with " + currentNode + " : " + compare);

                    if (compare > 0) {
                        Log.d("CP_insert","key's hash greater than current node's hash. continue");
                        continue;
                    } else {
                        // insert to global map
                        String port = activeNodes.get(currentNode);
                        Log.d("CP_insert", "Inserting to global map : " + currentNode + " with port : " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + value, port).get();
                        return uri;
                    }
                }
            }
        } catch (Exception e) {
            Log.d("CP_insert", "Exception : " + e.getMessage());
        }
        return uri;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        try {
            String key = selection;
            String keyHash = genHash(key);
            if (selection.equalsIgnoreCase("*")) {
                Log.d("CP_delete", "Global delete_all ");
                for (String port : activeNodes.values()) {
                    if (port.equalsIgnoreCase(myPort)) {
                        Log.d("CP_delete", "Global delete_all : deleting from port : " + myPort);
                        MapDAO.getInstance().getData().clear();
                    } else {
                        Log.d("CP_delete", "Global delete_all : deleting from port : " + port);
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE_ALL", port);
                    }
                }
            } else if (selection.equalsIgnoreCase("@")) {
                Log.d("CP_delete", "Local delete_all : ");
                MapDAO.getInstance().getData().clear();
                Log.d("CP_delete", "After Local delete_all : "+MapDAO.getInstance().getData());
            } else {
                // TODO what about the key that should be local but doesn't exist in the map
                String minHashKey = Collections.min(activeNodes.keySet());
                int lessThanMin = keyHash.compareTo(minHashKey);
                Log.d("CP_delete","Less than min = "+ lessThanMin);

                String maxHashKey = Collections.max(activeNodes.keySet());
                int moreThanMax = keyHash.compareTo(maxHashKey);
                Log.d("CP_delete", "More than max = "+moreThanMax);
                if (lessThanMin <= 0 || moreThanMax > 0) {
                    Log.d("CP_delete", "Delete from min hash node");
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE$" + key, activeNodes.get(minHashKey)).get();
                } else {
                    Log.d("CP_delete", "Global delete : key : " + keyHash);
                    Set<String> keySet = activeNodes.keySet();
                    Iterator<String> it = keySet.iterator();

                    while (it.hasNext()) {
                        String currentNode = it.next();
                        int compare = keyHash.compareTo(currentNode);

                        Log.d("CP_insert", "Comparing the key : " + keyHash + " with " + currentNode + " : " + compare);
                        if (compare > 0) {
                            Log.d("CP_delete","key's hash greater than current node's hash. continue");
                            continue;
                        } else {
                            // delete from global map
                            String port = activeNodes.get(currentNode);
                            Log.d("CP_delete", "Deleting from global map : key : " + currentNode + " at port : " + port);
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE$" + key, port).get();
                            break;
                        }
                    }
                }

                Log.d("CP_delete", "Values deleted : key " + key + " .... value " + value);
            }
        } catch (Exception e) {
            Log.d("CP_delete", "Exception : " + e.getMessage());
        }
        return 0;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {

        MatrixCursor cursor = null;

       // synchronized (this) {
            try {
                String key = selection;
                String keyHash = genHash(key);

                Log.d("CP_query", "Key to query : " + key);

                if (selection.equalsIgnoreCase("*")) {
                    Log.d("CP_query_global_all", "Global select_all ");
                    Map<String,String> data = new HashMap<String, String>();
                    for (String port : activeNodes.values()) {
                        if (port.equalsIgnoreCase(myPort)) {
                            Log.d("CP_query_all", "querying locally : " + myPort);
                            // Log.d("CP_query_all", "Global data size after adding data from port : " + myPort + " : " + qAllData.size());
                            data.putAll(MapDAO.getInstance().getData());

                        } else {
                            Log.d("CP_query_all", "Global select_all : querying from port : " + port);
                            // Log.d("CP_query_all", "Global data size : before adding data from port : " + port + " : " + qAllData.size());
                            StringBuilder result = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ALL", port).get();
                            Log.d("CP_query_all", "reponse from port : " + port + " : " + result);

                            Log.d("CP_query_all", "Parsing the response");
                            Map<String, String> dataFromOtherNode = parseQueryAllString(result.toString());
                            Log.d("CP_query_all", " data after parsing the result from node : "+port+" : "+dataFromOtherNode);
                            data.putAll(dataFromOtherNode);
                        }
                    }
                    Log.d("CP_query_all", "Global data size : after iterating all the nodes : " + qAllData.size());
                    Log.d("CP_query_all", "Global data : after iterating all the nodes : " + qAllData);
                    // Log.d("CP_query_all", "Global data : after iterating all the nodes : " + qAllData);
                    cursor = addValuesToCursor(data);
                    cursor.close();
                    return cursor;

                } else if (selection.equalsIgnoreCase("@")) {
                    Log.d("CP_query_local_all", "Local select_all");
                    cursor = addValuesToCursor(MapDAO.getInstance().getData());
                    return cursor;
                } else {
                    //....§
                    String minHashKey = Collections.min(activeNodes.keySet());
                    int lessThanMin = keyHash.compareTo(minHashKey);
                    Log.d("CP_query_one","Less than min = "+ lessThanMin);

                    String maxHashKey = Collections.max(activeNodes.keySet());
                    int moreThanMax = keyHash.compareTo(maxHashKey);
                    Log.d("CP_query_one", "More than max = "+moreThanMax);

                    if (lessThanMin <= 0 || moreThanMax > 0) {
                        Log.d("CP_query_one", "Query from min hash node");
                        StringBuilder result = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ONE$" + key, activeNodes.get(minHashKey)).get();
                        Log.d("CP_query_one", "Response received from min hash node : "+result);
                        Map<String, String> data = getQueryOneData(result.toString());
                        Log.d("CP_query_one", "Data after parsing the result : "+data);
                        cursor = addValuesToCursor(data);
                        cursor.close();
                        return cursor;
                    } else {
                        Log.d("CP_query_global", "Global query one");
                        Set<String> keySet = activeNodes.keySet();
                        Iterator<String> it = keySet.iterator();

                        while (it.hasNext()) {
                            String currentNode = it.next();
                            int compare = keyHash.compareTo(currentNode);
                            Log.d("CP_query_global", "Comparing the key hash : " + keyHash + " with " + currentNode + " : " + compare);
                            if (compare > 0) {
                                Log.d("CP_query_global","not querying the given node. continue");
                                continue;
                            } else {
                                // insert to global map
                                String port = activeNodes.get(currentNode);
                                Log.d("CP_query_global", "Querying global map for : " + currentNode + " at port : " + port);
                                StringBuilder result = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ONE$" + key, port).get();
                                Log.d("CP_query_global", "Out of asynctask");

                                Map<String, String> data = getQueryOneData(result.toString());
                                Log.d("CP_query_global", "adding query data to cursor from other nodes : "+data);
                                cursor = addValuesToCursor(data);
                                cursor.close();
                                return cursor;
                            }
                        }
                    }
                }
            } catch (Exception e) {
                Log.d("CP_query", "Exception : " + e.getMessage());
            }
        Log.d("CP_query", "Shouldn't be here");
        return cursor;
    }

    private Map<String, String> getQueryOneData(String resp) {
        Map<String, String> data = new HashMap<String, String>();

        if(resp.contains("QOR")) {
            Log.d("Client_Parse","Message is Query One Response : "+resp);
            String split[] = resp.split("@");
            Log.d("Client_Parse","Split [] : "+split);
            if(split[1] != null && !split[1].isEmpty() && split[1].contains("|")) {
                String parts[] = split[1].split("\\|");
                Log.d("Client_Parse","Parts [] : "+parts);
                data.put(parts[0],parts[1]);
                Log.d("Client_Parse", "Added data : "+parts[0]+" and "+parts[1]+" to QOR data");
            }
        }
        return data;
    }

    private static Map<String, String> parseQueryAllString(String result) {
        System.out.println(result);
        Map<String,String> data = new HashMap<String, String>();
        if(result == null || result.isEmpty()) {
            return data;
        } else {
            if(result.equalsIgnoreCase("ACK#")) {
                Log.d("parseQueryAllString", "response is 'ACK#' No data to parse. returning empty map");
                return data;
            } else if(result.contains("#")) {
                Log.d("parseQueryAllString", "response contains '#' splitting the response : "+result+ " by #");
                String pParts[] = result.split("\\#");
                result = pParts[1];
                Log.d("parseQueryAllString", "result : "+result);
            }

            if(result == null || result.isEmpty()) {
                Log.d("parseQueryAllString", "No data to parse. returning empty map");
                return data;
            } else if( !result.contains("@")){
                Log.d("parseQueryAllString", "Query response to parse one result : "+result);
                String parts [] = result.split("\\|");
                data.put(parts[0],parts[1]);
            } else {
                Log.d("parseQueryAllString", "Multiple query results to parse : "+result);
                StringTokenizer tokenizer = new StringTokenizer(result, "@");

                while(tokenizer.hasMoreElements()) {
                    String next = tokenizer.nextElement().toString();
                    Log.d("parseQueryAllString", "Next key val pair : "+next);
                    String parts [] = next.split("\\|");
                    Log.d("parseQueryAllString", "Next key val pair after split : "+parts);
                    data.put(parts[0],parts[1]);
                }
            }
            Log.d("parseQueryAllString","##########Parsed data size: "+data.size()+" data : "+data);
            return data;
        }
    }

    public MatrixCursor addValuesToCursor(Map<String, String> data) {
        String[] columnNames = new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";
        MatrixCursor cursor = new MatrixCursor(columnNames);

        Log.d("addValuesToCursor", "Data is : " + data);
        for (String key : data.keySet()) {
            String[] rowValues = new String[2];
            rowValues[0] = key;
            rowValues[1] = data.get(key);

            Log.d("addValuesToCursor", "Adding key : " + key + " and value : " + data.get(key));
            cursor.addRow(rowValues);
        }

        cursor.close();
        return cursor;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {
                    Log.d("Server", "Waiting for new messages");
                    Socket s = serverSocket.accept();
                    Log.d("Server", "Connection established");
                    InputStream is = s.getInputStream();
                    OutputStream os = s.getOutputStream();
                    Log.d("Server", "Got input and output streams");
                    InputStreamReader isr = new InputStreamReader(is);
                    PrintWriter pw = new PrintWriter(os, true);
                    BufferedReader br = new BufferedReader(isr);

                    String line = "";
                    String msg = "";

                    while ((line = br.readLine()) != null && !line.isEmpty()) {
                        Log.d("Server", "Reading data in loop");
                        msg += line;
                        Log.d("Server", "Read line : " + line);
                        break;
                    }

                    Log.d("Port is : ", s.getPort() + "");
                    Log.d("Server", "Message read is : " + msg);

                    if (msg.trim().contains("NODE_JOIN")) {
                        // add node to active nodes list
                        boolean added = addNodeToList(msg);
                        String resp = "ACK#" + added;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("UPDATE_ACTIVE_NODES")) {
                        // add node to active nodes list
                        boolean updated = updateActiveNodesList(msg);
                        String resp = "ACK#" + updated;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("INSERT")) {
                        // add entry to the map
                        boolean inserted = insertEntry(msg);
                        String resp = "ACK#" + inserted;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("DELETE")) {
                        // delete entry from the map
                        boolean deleted = deleteEntry(msg);
                        String resp = "ACK#" + deleted;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("QUERY_ONE")) {
                        // query one entry from the map
                        String queried = queryOne(msg);
                        String resp = "ACK#" + queried;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("QUERY_ALL")) {
                        // query all entries from the map
                        String query = queryAll(msg);
                        String resp = "ACK#" + query;
                        Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else {
                        // msg = msg + line;
                        pw.println("ACK");
                        Log.d("Server", "Responded : " + "ACK");
                        String[] msgs = new String[10];
                        msgs[0] = msg;
                        // publishProgress(msgs);
                        Log.d("Server", "Publishing the msg : " + msg);
                    }

                    is.close();
                    isr.close();
                    os.close();
                    pw.close();
                    br.close();
                    s.close();
                    Log.d("Server", "Socket and streams closed");
                }
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(TAG, "IO exception : " + e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(TAG, "Interrupted Exception : " + e.getMessage());
            } finally {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }

        private boolean addNodeToList(String message) throws NoSuchAlgorithmException {

            Log.d("Server_Master", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Master", "Message split is : " + parts);

            if (activeNodes.containsValue(port)) {
                Log.d("Server_Master", "The node is already listed on master");
                return false;
            }

            String nodeId = genHash(Long.valueOf(port)/2+"");
            activeNodes.put(nodeId, port);

            Log.d("Server_Master", "New node added with node id : " + nodeId + " for port : " + port);

            Iterator<String> it = activeNodes.keySet().iterator();

        /*if (activeNodes.size() <= 2) {
            successor = nodeId;
            Log.d("Server_Master", "Successor is : " + successor + " with port num : " + activeNodes.get(successor));
        }*/

            StringBuilder messageBuilder = new StringBuilder();
            Iterator<String> iterator = activeNodes.keySet().iterator();

            String firstKey = iterator.next();
            messageBuilder.append(activeNodes.get(firstKey));
            while (iterator.hasNext()) {
                String nextKey = iterator.next();
                messageBuilder.append("|" + activeNodes.get(nextKey));
            }

            Log.d("Server_Master", "Message to be sent to all nodes to update their tables : " + messageBuilder.toString());

            for (String key : activeNodes.keySet()) {
                if (!activeNodes.get(key).equals(myPort)) {
                    final String[] msgs = new String[2];
                    msgs[0] = "UPDATE_ACTIVE_NODES$" + messageBuilder.toString();
                    msgs[1] = activeNodes.get(key);
                    Log.d("Server_Master", "Message array sent to client task is : " + msgs);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgs[0], msgs[1]);
                    Log.d("Server_Master", "Message sent to node with node id : " + key + " and port number : " + activeNodes.get(key));
                }
            }

            // Log.d("Server_Master", "Thread spawned to send active nodes to each node");
            Log.d("Server_Master", "Done. Returning");
            return true;
        }

        private boolean updateActiveNodesList(String message) throws NoSuchAlgorithmException {

            boolean firstTime = false;
            String predecessor = "";
            if(activeNodes.isEmpty()) {
                firstTime = true;
            }

            Log.d("Server_Slave", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Slave", "Message split is : " + parts);
            Log.d("Server_Slave", "Message to parse : " + parts[1]);

            if (parts[1] == null || parts[1].isEmpty() || !parts[1].contains("UPDATE_ACTIVE_NODES$")) {
                Log.d("Server_Slave", "Message '" + parts[1] + "' cannot be parsed");
                return false;
            } else {
                Log.d("Server_Slave", "Message to be split is : " + parts[1]);
                String[] newParts = parts[1].split("\\$");
                Log.d("Server_Slave", "First split is : " + newParts[0]);
                Log.d("Server_Slave", "Second split is : " + newParts[1]);

                if (newParts[1] == null || newParts[1].isEmpty()) {
                    Log.d("Server_Slave", "Second split cannot be parsed");
                    return false;
                } else {
                    String[] nodes = newParts[1].split("\\|");
                    for (String node : nodes) {
                        int nodeBy2 = Integer.parseInt(node)/2;
                        String key = genHash(nodeBy2+"");

                        Log.d("Server_Slave", "Checking if a predecessor has been added");
                        if(firstTime == false && !activeNodes.containsKey(key)) {
                            int portInt = Integer.parseInt(myPort);
                            portInt = portInt/2;
                            if(genHash(nodeBy2+"").compareTo(genHash(portInt+"")) <= 0) {
                                Log.d("Server_Slave", "Predecessor is : "+node);
                                predecessor = node;
                            }
                        }

                        activeNodes.put(key, node);
                        Log.d("Server_Slave", "Active nodes ---- Added key : " + key + " and port : " + node);
                        Log.d("Server_Slave", "Active nodes ---- Current : " + activeNodes);
                    }
                }
            }

            if(!predecessor.isEmpty()) {
                Log.d("Server_Slave", "A new node has been added before the ");
                Map<String,String> toSend = new HashMap<String, String>();

                for(String key : MapDAO.getInstance().getData().keySet()) {
                    if(key.compareTo(predecessor) <= 0) {
                        toSend.put(key, MapDAO.getInstance().getData().get(key));
                    }
                }
                Log.d("Server_Slave", "Number of Messages to transfer : "+toSend.size()+" : messages : "+toSend);
                for(String key: toSend.keySet()) {
                    try {
                        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + toSend.get(key), activeNodes.get(predecessor)).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    MapDAO.getInstance().getData().remove(key);
                }
                Log.d("Server_Slave", "Number of Messages left after redistribution : "+MapDAO.getInstance().getData().size());
                Log.d("Server_Slave", "Messages left after redistribution --- "+MapDAO.getInstance().getData());
            }

            return true;
        }

        private boolean insertEntry(String message) throws NoSuchAlgorithmException {

            Log.d("Server_Insert", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Insert", "Message split is : " + parts);
            Log.d("Server_Insert", "Message to parse : " + parts[1]);

            if (parts[1] == null || parts[1].isEmpty() || !parts[1].contains("INSERT$")) {
                Log.d("Server_Insert", "Message '" + parts[1] + "' cannot be parsed");
                return false;
            } else {
                Log.d("Server_Insert", "Message to be split is : " + parts[1]);
                String[] newParts = parts[1].split("\\$");
                Log.d("Server_Insert", "First split is : " + newParts[0]);
                Log.d("Server_Insert", "Second split is : " + newParts[1]);

                if (newParts[1] == null || newParts[1].isEmpty()) {
                    Log.d("Server_Insert", "Second split cannot be parsed");
                    return false;
                } else {
                    String[] keyVal = newParts[1].split("\\|");

                    String key = keyVal[0];
                    String value = keyVal[1];

                    Log.d("Server_Insert", "Hashed key : "+key);

                    Log.d("Server_Insert", "Unhashed key : "+genHash(key));

                    MapDAO.getInstance().getData().put(key, value);

                    Log.d("Server_Insert", "Added key : " + key + " and value : " + value);
                    Log.d("Server_Insert", "Current data ---- size : "+MapDAO.getInstance().getData().size()+" : " + MapDAO.getInstance().getData());
                }
            }
            return true;
        }

        private boolean deleteEntry(String message) throws NoSuchAlgorithmException {

            Log.d("Server_Delete", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Delete", "Message split is : " + parts);
            Log.d("Server_Delete", "Message to parse : " + parts[1]);

            if (parts[1] == null || parts[1].isEmpty() || !parts[1].contains("DELETE$")) {
                Log.d("Server_Delete", "Message '" + parts[1] + "' cannot be parsed");
                return false;
            } else {
                Log.d("Server_Delete", "Message to be split is : " + parts[1]);
                String[] newParts = parts[1].split("\\$");
                Log.d("Server_Delete", "First split is : " + newParts[0]);
                Log.d("Server_Delete", "Second split is : " + newParts[1]);

                if (newParts[1] == null || newParts[1].isEmpty()) {
                    Log.d("Server_Delete", "Second split cannot be parsed");
                    return false;
                } else {
                    String keyHash = newParts[1];

                    String toRemove = MapDAO.getInstance().getData().get(keyHash);
                    Log.d("Server_Delete", "To remove from map : key : " + keyHash + " value : " + toRemove);

                    MapDAO.getInstance().getData().remove(keyHash);
                    Log.d("Server_Delete", "Removed key : " + keyHash + " and value : " + toRemove);
                    Log.d("Server_Delete", "Current data ---- : " + MapDAO.getInstance().getData());
                }
            }
            return true;
        }

        private String queryOne(String message) throws NoSuchAlgorithmException {
            Log.d("Server_Query_one", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Query_one", "Message split is : " + parts);
            Log.d("Server_Query_one", "Message to parse : " + parts[1]);

            if (parts[1] == null || parts[1].isEmpty() || !parts[1].contains("QUERY_ONE$")) {
                Log.d("Server_Query_one", "Message '" + parts[1] + "' cannot be parsed");
                return "";
            } else {
                Log.d("Server_Query_one", "Message to be split is : " + parts[1]);
                String[] newParts = parts[1].split("\\$");
                Log.d("Server_Query_one", "First split is : " + newParts[0]);
                Log.d("Server_Query_one", "Second split is : " + newParts[1]);

                if (newParts[1] == null || newParts[1].isEmpty()) {
                    Log.d("Server_Query_one", "Second split cannot be parsed");
                    return "";
                } else {
                    String keyHash = newParts[1];
                    String toReturn = MapDAO.getInstance().getData().get(keyHash);

                    Log.d("Server_Query_one", "To query from map : key : " + keyHash + " value : " + toReturn);
                    Log.d("Server_Query_one", "Queried key : " + keyHash + " and value : " + toReturn);
                    Log.d("Server_Query_one", "Current data ---- : size : "+MapDAO.getInstance().getData().size()+" : " + MapDAO.getInstance().getData());

                    return "QOR@"+keyHash+"|"+toReturn;
                }
            }
        }

        private String queryAll(String message) throws NoSuchAlgorithmException {
            Log.d("Server_Query_All", "Message received : " + message);

            String result = "";

            Set<String> keySet = MapDAO.getInstance().getData().keySet();
            Iterator<String> it = keySet.iterator();
            if(keySet.isEmpty()) {
                Log.d("Server_Query_All","No data in map : "+result);
                // return "";
            } else {

                String nextKey = "";
                nextKey = it.next();
                result = nextKey+"|"+MapDAO.getInstance().getData().get(nextKey);

                while(it.hasNext()) {
                    nextKey = it.next();
                    result = result+"@"+nextKey+"|"+MapDAO.getInstance().getData().get(nextKey);
                }
                Log.d("Server_Query_All","Replying result string : "+result);
            }

            Log.d("Server_Query_All", "Number of messages replied  : " + MapDAO.getInstance().getData().size());
            Log.d("Server_Query_All", "Messages replied  : " + MapDAO.getInstance().getData());
            Log.d("Server_Query_All", "Number of messages replied  as string : " + result);
            return result;
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

    private class ClientTask extends AsyncTask<String, String, StringBuilder> {

        @Override
        protected StringBuilder doInBackground(String... msgs) {

            StringBuilder response = new StringBuilder();
            try {
                String message = msgs[0];
                String remotePort = msgs[1];
                Log.d("Client", "Message to be sent : " + message + " | remote port : " + remotePort);
                response = send(message, remotePort);
                Log.d("Client", "Response received from server for message : " + message + " is : " + response.toString());
            } catch (UnknownHostException e) {
                Log.e("Client", "UnknownHostException");
            } catch (SocketException e) {
                Log.e("Client", "Socket Exception : " + e.getMessage());
                e.printStackTrace();
            } catch (IOException e) {
                Log.e("Client", "IOException : " + e.getMessage());
            } catch (Exception e) {
                Log.e("Client", "Excpetion : " + e.getMessage());
                e.printStackTrace();
            }
            return response;
        }

        private StringBuilder send(String message, String remotePort) throws SocketException, IOException, InterruptedException {
            //String resp = "";
            StringBuilder response = new StringBuilder();
            if (message == null || message.isEmpty()) {
                Log.d("Client", "Request empty. Not sent to server");
                return response;
            }

            try {
                Log.d("Client", "About to create server connection 1");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                // socket.setSoTimeout(500);

                Log.d("Client", "Server socket created 1");
                OutputStream os = socket.getOutputStream();
                InputStream is = socket.getInputStream();

                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                Log.d("Client", "Got output stream 1");
                PrintWriter pw = new PrintWriter(os, true);

                Log.d("Client", "Sending message : " + myPort + "*" + message);
                pw.println(myPort + "*" + message);

                // Thread.sleep(2000);

                String lineAck = "";
                while ((lineAck = br.readLine()) != null && !lineAck.isEmpty()) {
                    // Thread.sleep(10);
                    Log.d("Client", "Reading ack in loop: " + lineAck);
                    response.append(lineAck);
                    break;
                }

                Log.d("Client", "Ack received : " + response.toString());

                os.close();
                is.close();
                socket.close();
            } catch (IOException e) {
                if (e instanceof SocketTimeoutException) {
                    int client = Log.d("Client", "Socket time out exception for port : " + remotePort);
                }
                e.printStackTrace();
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            return response;
        }

    }
}
