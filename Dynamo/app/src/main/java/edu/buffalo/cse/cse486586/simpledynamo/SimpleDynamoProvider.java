package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static android.R.attr.syncable;
import static android.R.attr.value;
import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {

    private String myPort;
    private Map<String, Node> activeNodes = new TreeMap<String, Node>();

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        Log.d("CP_create", "My Port is : " + myPort);
        try {
            ServerSocket serverSocket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d("CP_create", "Server Socket started");
        } catch (IOException e) {
            Log.e("CP_create", "Can't create a ServerSocket. e = " + e.getMessage());
        }

        // generate hash for all nodes and add them to active nodes map
        Log.d("CP_create", "Setting up initial nodes");
        createInitialActiveNodes();

        // send node join request to all nodes
        final String[] msgs = new String[2];
        msgs[0] = "NODE_JOIN";
        Map<String, DynamoMessage> data = MapDAO.getInstance().getData();
        for (Node node : activeNodes.values()) {
            if (!node.getPort().equalsIgnoreCase(myPort)) {
                // Log.d("CP_create", "node join : port : " + node.getPort());
                msgs[1] = node.getPort();
                try {
                    Log.d("CP_create", "Sending node join message to port : " + node.getPort());
                    // TODO send a ping message
                    StringBuilder response = new StringBuilder();
                    long startingTime = System.currentTimeMillis();
                    while (response.toString().isEmpty()) {
                        SystemClock.sleep(800);
                        long now = System.currentTimeMillis();
                        if((now - startingTime) > 5000) {
                            Log.d("CP_create", "No response from port : "+node.getPort()+" . Will check other nodes");
                            break;
                        }
                        Log.d("CP_create", "Sending Node Join Message to : " + node.getPort());
                        response = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgs[0], msgs[1]).get();
                    }
                    Log.d("CP_create", "Response received from port : " + node.getPort() + " is : " + response.toString());
                    parseAndAddQAllResponse(data, response.toString());
                } catch (InterruptedException e) {
                    Log.e("CP_create", "Interrupted..." + e.getMessage());
                } catch (ExecutionException e) {
                    Log.e("CP_create", "Execution Exception..." + e.getMessage());
                }
            }
        }
        Log.d("CP_create", "Total size of recovered messages : " + data.size() + " with data : " + data);

        return true;
    }

    private void createInitialActiveNodes() {

        LinkedList<Node> nodes = new LinkedList<Node>();
        try {
            nodes.add(new Node("5554"));
            nodes.add(new Node("5556"));
            nodes.add(new Node("5558"));
            nodes.add(new Node("5560"));
            nodes.add(new Node("5562"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Collections.sort(nodes);
        System.out.println("Initial nodes : " + nodes);

        Iterator<Node> it = nodes.iterator();
        Node n1 = it.next();
        while (it.hasNext()) {
            Node n2 = it.next();
            n1.setSuccessor(n2);
            n2.setPredecessor(n1);
            n1 = n2;
        }

        Node minNode = Collections.min(nodes);
        Node maxNode = Collections.max(nodes);
        minNode.setPredecessor(maxNode);
        maxNode.setSuccessor(minNode);

        for (Node n : nodes) {
            activeNodes.put(n.getHash(), n);
        }
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        synchronized (this) {
            try {
                String key = selection;
                String keyHash = genHash(key);
                if (selection.equalsIgnoreCase("*")) {
                    Log.d("CP_delete", "Global delete_all ");
                    for (Node node : activeNodes.values()) {
                        if (node.getPort().equalsIgnoreCase(myPort)) {
                            Log.d("CP_delete", "Global delete_all : deleting from port : " + myPort);
                            MapDAO.getInstance().getData().clear();
                        } else {
                            Log.d("CP_delete", "Global delete_all : deleting from port : " + node.getPort());
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE_ALL", node.getPort());
                        }
                    }
                } else if (selection.equalsIgnoreCase("@")) {
                    Log.d("CP_delete", "Local delete_all : ");
                    MapDAO.getInstance().getData().clear();
                    Log.d("CP_delete", "After Local delete_all : " + MapDAO.getInstance().getData());
                } else {
                    // TODO what about the key that should be local but doesn't exist in the map. probably fixed
                    String minHashKey = Collections.min(activeNodes.keySet());
                    int lessThanMin = keyHash.compareTo(minHashKey);
                    String maxHashKey = Collections.max(activeNodes.keySet());
                    int moreThanMax = keyHash.compareTo(maxHashKey);

                    if (lessThanMin <= 0 || moreThanMax > 0) {
                        Node node = activeNodes.get(minHashKey);
                        Log.d("CP_delete", "Deleting from min hash node : " + node.getPort() + " and it's successors " + node.getSuccessor().getPort() + " and " + node.getSuccessor().getSuccessor().getPort());
                        deleteFromNodeAndSuccessors(node, key);
                    } else {
                        Log.d("CP_delete", "Global delete : key : " + keyHash);
                        Set<String> keySet = activeNodes.keySet();
                        Iterator<String> it = keySet.iterator();
                        while (it.hasNext()) {
                            String currentNode = it.next();
                            int compare = keyHash.compareTo(currentNode);
                            // Log.d("CP_delete", "Comparing the key : " + keyHash + " with " + currentNode + " : " + compare);
                            if (compare > 0) {
                                // Log.d("CP_delete", "key's hash greater than current node's hash. continue");
                                continue;
                            } else {
                                Node node = activeNodes.get(currentNode);
                                Log.d("CP_delete", "Deleting from node : " + node.getPort() + " and it's successors " + node.getSuccessor().getPort() + " and " + node.getSuccessor().getSuccessor().getPort());
                                deleteFromNodeAndSuccessors(node, key);
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
    }

    private synchronized void deleteFromNodeAndSuccessors(Node node, String key) throws InterruptedException, ExecutionException {
        Log.d("CP_delete", "Deleting from primary node : " + node.getSuccessor().getPort());
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE$" + key, node.getPort()).get();
        if (node.getSuccessor().isActive()) {
            Log.d("CP_delete", "Deleting from first successor : " + node.getSuccessor().getPort());
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE$" + key, node.getSuccessor().getPort()).get();
            Log.d("CP_delete", "Deletion complete on first successor : " + node.getSuccessor().getPort());
        }
        if (node.getSuccessor().getSuccessor().isActive()) {
            Log.d("CP_delete", "Deleting from second successor : " + node.getSuccessor().getSuccessor().getPort());
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "DELETE$" + key, node.getSuccessor().getSuccessor().getPort()).get();
            Log.d("CP_delete", "Deletion complete on second successor : " + node.getSuccessor().getPort());
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        synchronized (this) {

            try {
                String key = values.get("key").toString();
                String value = (String) values.get("value");
                String keyHash = genHash(key);

                Log.d("CP_insert", "Values to insert : key " + key + " .... value " + value);

                String minHashKey = Collections.min(activeNodes.keySet());
                int lessThanMin = keyHash.compareTo(minHashKey);
                // Log.d("CP_insert", "Less than min = " + lessThanMin);

                String maxHashKey = Collections.max(activeNodes.keySet());
                int moreThanMax = keyHash.compareTo(maxHashKey);
                // Log.d("CP_insert", "More than max = " + moreThanMax);

                if (lessThanMin <= 0 || moreThanMax > 0) {
                    Node node = activeNodes.get(minHashKey);
                    Log.d("CP_insert", "Inserting to min hash node : " + node.getPort() + " and it's successors " + node.getSuccessor().getPort() + " and " + node.getSuccessor().getSuccessor().getPort());
                    insertToNodeAndSuccessors(node, key, value);
                    return uri;
                } else {
                    Set<String> keySet = activeNodes.keySet();
                    Iterator<String> it = keySet.iterator();
                    while (it.hasNext()) {
                        String currentNode = it.next();
                        int compare = keyHash.compareTo(currentNode);
                        // Log.d("CP_insert", "Comparing the key : " + keyHash + " with " + currentNode + " : " + compare);
                        if (compare > 0) {
                            Log.d("CP_insert", "key's hash greater than current node's hash. continue");
                            continue;
                        } else {
                            // insert to global map
                            Node node = activeNodes.get(currentNode);
                            Log.d("CP_insert", "Inserting to node : " + currentNode + " with port : " + node.getPort() + " and it's successors " + node.getSuccessor().getPort() + " and " + node.getSuccessor().getSuccessor().getPort());
                            insertToNodeAndSuccessors(node, key, value);
                            return uri;
                        }
                    }
                }
            } catch (Exception e) {
                Log.d("CP_insert", "Exception : " + e.getMessage());
            }
            return uri;
        }
    }

    private synchronized void insertToNodeAndSuccessors(Node node, String key, String value) throws InterruptedException, ExecutionException {
        new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + value + "|false|" + node.getPort(), node.getPort()).get();
        if (node.getSuccessor().isActive()) {
            Log.d("CP_insert", "Inserting to first successor : " + node.getSuccessor().getPort());
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + value + "|true|" + node.getPort(), node.getSuccessor().getPort()).get();
            Log.d("CP_insert", "Insertion complete on first successor : " + node.getSuccessor().getPort());
        }

        if (node.getSuccessor().getSuccessor().isActive()) {
            Log.d("CP_insert", "Inserting to second successor : " + node.getSuccessor().getSuccessor().getPort());
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "INSERT$" + key + "|" + value + "|true|" + node.getPort(), node.getSuccessor().getSuccessor().getPort()).get();
            Log.d("CP_insert", "Insertion complete on second successor : " + node.getSuccessor().getPort());
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        synchronized (this) {
            MatrixCursor cursor = null;
            try {
                String key = selection;
                String keyHash = genHash(key);
                Log.d("CP_query", "Key to query : " + key);

                if (selection.equalsIgnoreCase("*")) {
                    Log.d("CP_query_global_all", "Global select_all ");
                        Map<String, DynamoMessage> data = new HashMap<String, DynamoMessage>();
                        for (Node node : activeNodes.values()) {
                            if (node.getPort().equalsIgnoreCase(myPort)) {
                                Log.d("CP_query_all", "querying local data from : " + myPort);

                                Map<String, DynamoMessage> localData = MapDAO.getInstance().getData();
                                for (String localKey : localData.keySet()) {
                                    if (data.containsKey(localKey)) {
                                        if (localData.get(localKey).getVersion() > data.get(localKey).getVersion()) {
                                            data.put(localKey, localData.get(localKey));
                                        }
                                    } else {
                                        data.put(localKey, localData.get(localKey));
                                    }
                                }
                                Log.d("CP_query_all", "Added all local data from : " + myPort);
                            } else {
                                Log.d("CP_query_all", "Global: querying from port : " + node.getPort());
                                // Log.d("CP_query_all", "Global data size : before adding data from port : " + port + " : " + qAllData.size());
                                StringBuilder result = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ALL", node.getPort()).get();
                                Log.d("CP_query_all", "Global: Response from port : " + node.getPort() + " : " + result);
                                parseAndAddQAllResponse(data, result.toString());
                            }
                        }
                        Log.d("CP_query_all", "Global data after iterating all the nodes : size : " + data.size() + " data : " + data);
                        cursor = addValuesToCursor(data);
                        cursor.close();
                        return cursor;
                } else if (selection.equalsIgnoreCase("@")) {
                    Log.d("CP_query_local_all", "Local select_all result size = " + MapDAO.getInstance().getData().size());
                    // TODO check if versioning needs to be taken care here too. probably not
                    cursor = addValuesToCursor(MapDAO.getInstance().getData());
                    return cursor;
                } else {
                    String minHashKey = Collections.min(activeNodes.keySet());
                    int lessThanMin = keyHash.compareTo(minHashKey);
                    // Log.d("CP_query_one", "Less than min = " + lessThanMin);

                    String maxHashKey = Collections.max(activeNodes.keySet());
                    int moreThanMax = keyHash.compareTo(maxHashKey);
                    // Log.d("CP_query_one", "More than max = " + moreThanMax);

                    Map<String, DynamoMessage> data = new HashMap<String, DynamoMessage>();
                    if (lessThanMin <= 0 || moreThanMax > 0) {
                        Log.d("CP_query_one", "Query from min hash node");
                        queryOneFromDynamo(data, key, activeNodes.get(minHashKey));
                        cursor = addValuesToCursor(data);
                        cursor.close();
                        return cursor;
                    } else {
                        Log.d("CP_query_global", "Global query for one key");
                        synchronized (this) {
                            Set<String> keySet = activeNodes.keySet();
                            Iterator<String> it = keySet.iterator();
                            while (it.hasNext()) {
                                String currentNode = it.next();
                                int compare = keyHash.compareTo(currentNode);
                                // Log.d("CP_query_global", "Comparing the key hash : " + keyHash + " with " + currentNode + " : " + compare);
                                if (compare > 0) {
                                    // Log.d("CP_query_global", "need not query the given node. continue");
                                    continue;
                                } else {
                                    queryOneFromDynamo(data, key, activeNodes.get(currentNode));
                                    cursor = addValuesToCursor(data);
                                    cursor.close();
                                    return cursor;
                                }
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
    }

    private synchronized void queryOneFromDynamo(Map<String, DynamoMessage> data, String key, Node node) throws InterruptedException, ExecutionException {
        StringBuilder result = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ONE$" + key, node.getPort()).get();
        parseAndAddQAllResponse(data, result.toString());
        StringBuilder resultFS = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ONE$" + key, node.getSuccessor().getPort()).get();
        parseAndAddQAllResponse(data, resultFS.toString());
        StringBuilder resultSS = new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, "QUERY_ONE$" + key, node.getSuccessor().getSuccessor().getPort()).get();
        parseAndAddQAllResponse(data, resultSS.toString());
    }

    private synchronized void parseAndAddQAllResponse(Map<String, DynamoMessage> data, String result) {
        Log.d("parseAndAddQAllResponse", "Will parse data from string : " + result);
        if (result == null || result.isEmpty()) {
            Log.d("parseAndAddQAllResponse", "Empty message. Nothing to parse");
            return;
        } else {
            if (result.equalsIgnoreCase("ACK#")) {
                Log.d("parseAndAddQAllResponse", "response is 'ACK#' No data to parse. returning empty map");
                return;
            } else if (result.contains("#")) {
                Log.d("parseAndAddQAllResponse", "response contains '#' splitting the response : " + result + " by #");
                String pParts[] = result.split("\\#");
                result = pParts[1];
                Log.d("parseAndAddQAllResponse", "result : " + result);
            }

            if (result == null || result.isEmpty()) {
                Log.d("parseAndAddQAllResponse", "No data to parse from message : " + result);
                return;
            } else if (result.contains("NJ_RESPONSE")) {
                Log.d("parseAndAddQAllResponse", "Query response contains NJ_RESPONSE : " + result);
                String pParts[] = result.split("\\*");
                result = pParts[1];
            } else if (result.contains("QUERY_ONE_RESPONSE")) {
                Log.d("parseAndAddQAllResponse", "Query response contains QUERY_ONE_RESPONSE : " + result);
                String pParts[] = result.split("\\@");
                result = pParts[1];
            }

            if (result == null || result.isEmpty()) {
                Log.d("parseAndAddQAllResponse", "No data to parse. returning empty map");
                return;
            } else if (!result.contains("@")) {
                Log.d("parseAndAddQAllResponse", "Query response to parse with one result : " + result);
                createDynamoMessageAndAddToResults(data, result);
            } else {
                Log.d("parseAndAddQAllResponse", "Response to parse with multiple results : " + result);
                StringTokenizer tokenizer = new StringTokenizer(result, "@");
                while (tokenizer.hasMoreElements()) {
                    String next = tokenizer.nextElement().toString();
                    Log.d("parseAndAddQAllResponse", "Next message to tokenize : " + next);
                    createDynamoMessageAndAddToResults(data, next);
                }
            }
            Log.d("parseAndAddQAllResponse", "##########Parsed data size: " + data.size() + " data : " + data);
            return;
        }
    }

    // this method will add message to query result if the msg does not already exist, or if it contains older version of message
    private synchronized void createDynamoMessageAndAddToResults(Map<String, DynamoMessage> data, String string) {

        if (string == null || string.isEmpty()) {
            return;
        }

        Log.d("createDynamoMessage", "Creating DynamoMessage from : " + string);
        StringTokenizer tokenizer = new StringTokenizer(string, "|");
        String[] tokens = new String[5];
        int i = 0;
        while (tokenizer.hasMoreElements()) {
            String next = tokenizer.nextElement().toString();
            tokens[i] = next;
            i++;
        }

        Log.d("createDynamoMessage", "Tokens : " + Arrays.asList(tokens));
        String key = tokens[0];
        String value = tokens[1];
        Boolean isReplicated = Boolean.valueOf(tokens[2]);
        String fromPort = tokens[3];
        Long newVersion = Long.valueOf(tokens[4]);

        //DynamoMessage dynamoMessage = new DynamoMessage(value,isReplicated,fromPort,newVersion);
        //Log.d("createDynamoMessage", "Created DynamoMessage : "+dynamoMessage);

        if (data.containsKey(key)) {
            long existingVersion = data.get(key).getVersion();
            if (newVersion > existingVersion && !(data.get(key).getMessage().equals(value))) {
                data.put(key, new DynamoMessage(value, isReplicated, fromPort, newVersion));
                Log.d("createDynamoMessage", "Newer version of the Message available for key : " + key + " . Added the new value : " + value + " replaced : " + data.get(key).getMessage());
            } else {
                Log.d("createDynamoMessage", "Not adding key : " + key + " and value : " + value);
            }
        } else {
            data.put(key, new DynamoMessage(value, isReplicated, fromPort, newVersion));
            Log.d("createDynamoMessage", "No entry exists for key : " + key + " . added value : " + value);
        }
    }

    private synchronized MatrixCursor addValuesToCursor(Map<String, DynamoMessage> data) {
        String[] columnNames = new String[2];
        columnNames[0] = "key";
        columnNames[1] = "value";
        MatrixCursor cursor = new MatrixCursor(columnNames);

        Log.d("addValuesToCursor", "Cursor will be added with data size : " + data.size() + " Data : " + data);
        for (String key : data.keySet()) {
            String[] rowValues = new String[2];
            rowValues[0] = key;
            rowValues[1] = data.get(key).getMessage();
            Log.d("addValuesToCursor", "Cursor, adding key : " + key + " and value : " + data.get(key).getMessage());
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
                        String response = activateNodeAndGetReplicatedMessages(msg);
                        String resp = "ACK#" + response;
                        // Log.d("Server", "Responded : " + resp);
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
                        boolean deleted = deleteOneFromMap(msg);
                        String resp = "ACK#" + deleted;
                        // Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("QUERY_ONE")) {
                        // query one entry from the map
                        String queried = getOneValueFromMap(msg);
                        String resp = "ACK#" + queried;
                        // Log.d("Server", "Responded : " + resp);
                        pw.println(resp);
                        Log.d("Server", "Responded : " + resp);
                    } else if (msg.trim().contains("QUERY_ALL")) {
                        // query all entries from the map
                        String query = getAllDataFromMap();
                        String resp = "ACK#" + query;
                        // Log.d("Server", "Responded : " + resp);
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

        private synchronized String activateNodeAndGetReplicatedMessages(String message) throws NoSuchAlgorithmException {

            Log.d("ActivateNode", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("ActivateNode", "Message split is : " + Arrays.asList(parts));

            Node node = activeNodes.get(genHash((Long.valueOf(port))/2+""));
            node.setActive(true);
            Log.d("ActivateNode", "Node Re-activated : " + node);

            String response = getAllReplicatedDataForNode(node);
            // activeNodes.put(nodeId, port);
            Log.d("ActivateNode", "Sending back response : '" + response + "' to node : " + node);
            return response;
        }

        private synchronized boolean insertEntry(String message) throws NoSuchAlgorithmException {
            Log.d("Server_Insert", "Message received : " + message);
            String[] parts = message.split("\\*");
            String port = parts[0];
            Log.d("Server_Insert", "Message split is : " + Arrays.asList(parts));
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
                    StringTokenizer tokenizer = new StringTokenizer(newParts[1], "|");
                    String[] tokens = new String[4];
                    int i = 0;
                    while (tokenizer.hasMoreElements()) {
                        String next = tokenizer.nextElement().toString();
                        tokens[i] = next;
                        i++;
                    }
                    Log.d("Server_Insert", "tokens obtained for insert : " + Arrays.asList(tokens));

                    String key = tokens[0];
                    String msg = tokens[1];
                    String isReplicated = tokens[2];
                    String fromPort = tokens[3];
                    MapDAO.getInstance().getData().put(key, new DynamoMessage(msg, isReplicated, fromPort));
                    Log.d("Server_Insert", "Saved key : " + key + " and value : " + msg + " from port " + fromPort + ", replicated = " + isReplicated + "\n Map size : " + MapDAO.getInstance().getData().size() + " : " + MapDAO.getInstance().getData());
                }
            }
            return true;
        }

        private synchronized boolean deleteOneFromMap(String message) throws NoSuchAlgorithmException {

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
                    String key = newParts[1];
                    DynamoMessage removed = MapDAO.getInstance().getData().remove(key);
                    if (removed == null) {
                        Log.d("Server_Delete", "No value found for key : " + key);
                    } else {
                        Log.d("Server_Delete", "Removed local copy of key : " + key + " value : " + removed);
                        Log.d("Server_Delete", "Size of map after removal of key : " + key + " is : " + MapDAO.getInstance().getData().size());
                    }

                }
            }
            return true;
        }

        private  synchronized  String getOneValueFromMap(String message) throws NoSuchAlgorithmException {
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
                    String key = newParts[1];
                    DynamoMessage dynamoMessage = MapDAO.getInstance().getData().get(key);
                    if(dynamoMessage == null) {
                        Log.d("Server_Query_one", "Key not found : "+key);
                        return "";
                    }
                    String msg = dynamoMessage.getMessage();
                    String isReplicated = MapDAO.getInstance().getData().get(key).isReplicated().toString();
                    String fromPort = MapDAO.getInstance().getData().get(key).getOwnerPort();
                    Long version = MapDAO.getInstance().getData().get(key).getVersion();
                    Log.d("Server_Query_one", "Queried for key : " + key + " got  value : " + msg + " replicated : " + isReplicated + " from port : " + fromPort + " version : " + version);
                    Log.d("Server_Query_one", "Current data ---- : size : " + MapDAO.getInstance().getData().size() + " : " + MapDAO.getInstance().getData());
                    return "QUERY_ONE_RESPONSE@" + key + "|" + msg + "|" + isReplicated + "|" + fromPort + "|" + version;
                }
            }
        }

        private  synchronized  String getAllDataFromMap() throws NoSuchAlgorithmException {
            String result = "";
            Set<String> keySet = MapDAO.getInstance().getData().keySet();
            Iterator<String> it = keySet.iterator();
            if (keySet.isEmpty()) {
                Log.d("Server_Query_All", "No data in the Map");
            } else {
                String nextKey = "";
                nextKey = it.next();
                result = nextKey + "|" + MapDAO.getInstance().getData().get(nextKey).getMessage() + "|" + MapDAO.getInstance().getData().get(nextKey).isReplicated() + "|" + MapDAO.getInstance().getData().get(nextKey).getOwnerPort() + "|" + MapDAO.getInstance().getData().get(nextKey).getVersion();
                while (it.hasNext()) {
                    nextKey = it.next();
                    result = result + "@" + nextKey + "|" + MapDAO.getInstance().getData().get(nextKey).getMessage() + "|" + MapDAO.getInstance().getData().get(nextKey).isReplicated() + "|" + MapDAO.getInstance().getData().get(nextKey).getOwnerPort() + "|" + MapDAO.getInstance().getData().get(nextKey).getVersion();
                }
            }
            Log.d("Server_Query_All", "Number of messages replied  : " + MapDAO.getInstance().getData().size() + " Messages replied  : " + MapDAO.getInstance().getData());
            Log.d("Server_Query_All", "Local Query All Response string : " + result);
            return result;
        }

        private  synchronized String getAllReplicatedDataForNode(Node node) throws NoSuchAlgorithmException {

            // TODO check if there are concurrency issues introduced on Map data
            String result = "";
            Map<String, DynamoMessage> data = MapDAO.getInstance().getData();

            if (data.size() == 0) {
                Log.d("GetReplicaData", "The map is empty at the moment. Nothing to return");
                return "";
            }

            Set<String> keySet = MapDAO.getInstance().getData().keySet();
            Map<String, DynamoMessage> replicatedData = new HashMap<String, DynamoMessage>();
            for (String key : keySet) {

                if(data.get(key) == null || data.get(key).getOwnerPort() == null || data.get(key).getOwnerPort().isEmpty()) {
                    Log.d("GetReplicaData", "Could not extract owner port from the message : "+data.get(key));
                    return "";
                }

                if((node.getPredecessor().getPort().equals(myPort) && data.get(key).getOwnerPort().equals(myPort))
                        || (node.getPredecessor().getPredecessor().getPort().equals(myPort) && data.get(key).getOwnerPort().equals(myPort))
                        || (node.getSuccessor().getPort().equals(myPort) && data.get(key).getOwnerPort().equals(node.getPort()))
                        || (node.getSuccessor().getSuccessor().getPort().equals(myPort) && data.get(key).getOwnerPort().equals(node.getPort()))) {

                    if (replicatedData.containsKey(key)) {
                        long existingVersion = replicatedData.get(key).getVersion();
                        long newVersion = data.get(key).getVersion();
                        if (newVersion > existingVersion && !(data.get(key).getMessage().equals(replicatedData.get(key).getMessage()))) {
                            replicatedData.put(key, data.get(key));
                            Log.d("GetReplicaData", "Adding data to Replicated Map with newer version of the Message ie key : " + key + " . Added the new value : " + replicatedData.get(key).getMessage() + " replaced : " + data.get(key).getMessage());
                        } else {
                            Log.d("GetReplicaData", "Not adding data to Replicated Map. key : " + key + " and value : " + data.get(key).getMessage());
                        }
                    } else {
                        replicatedData.put(key, data.get(key));
                        Log.d("GetReplicaData", "Adding data to Replicated Map. key : " + key + " and value : " + data.get(key).getMessage());
                    }
                }
            }

            if (replicatedData.isEmpty()) {
                Log.d("GetReplicaData", "No replicated data found for port : " + node.getPort());
                return "";
            } else {
                Log.d("GetReplicaData", "Replicated data found for port : " + node.getPort() + " size : " + replicatedData.size() + " data : " + replicatedData);
            }

            Iterator<String> it = replicatedData.keySet().iterator();
            String nextKey = it.next();
            result = nextKey + "|" + replicatedData.get(nextKey).getMessage() + "|" + replicatedData.get(nextKey).isReplicated() + "|" + replicatedData.get(nextKey).getOwnerPort() + "|" + replicatedData.get(nextKey).getVersion();
            while (it.hasNext()) {
                nextKey = it.next();
                //if (MapDAO.getInstance().getData().get(nextKey).getOwnerPort().equals(port)) {
                result = result + "@" + nextKey + "|" + replicatedData.get(nextKey).getMessage() + "|" + replicatedData.get(nextKey).isReplicated() + "|" + replicatedData.get(nextKey).getOwnerPort() + "|" + replicatedData.get(nextKey).getVersion();
                //}
            }

            Log.d("GetReplicaData", "Number of messages replied  : " + replicatedData.size() + " Messages replied  : " + replicatedData);
            return "NJ_RESPONSE*" + result;
        }

        private synchronized  String genHash(String input) throws NoSuchAlgorithmException {
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
                Log.d("Client", "Response received from server for message : " + message + " for port " + remotePort + " is : " + response.toString());
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

        private StringBuilder send(String message, String remotePort) throws SocketException, IOException, InterruptedException, NoSuchAlgorithmException {
            StringBuilder response = new StringBuilder();
            Node node = activeNodes.get(genHash(Long.valueOf(remotePort) / 2 + ""));
            if (!node.isActive()) {
                Log.d("Client", "The port : " + node.getPort() + " is inactive. Not sending the message. Check node : " + node);
                return response;
            }

            if (message == null || message.isEmpty()) {
                Log.d("Client", "Request empty. Not sent to server");
                return response;
            }

            try {
                Log.d("Client", "About to create server connection 1");
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                socket.setSoTimeout(500);

                Log.d("Client", "Server socket created");
                OutputStream os = socket.getOutputStream();
                InputStream is = socket.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                PrintWriter pw = new PrintWriter(os, true);

                Log.d("Client", "Sending message : " + myPort + "*" + message + " to port " + remotePort);
                pw.println(myPort + "*" + message);

                String lineAck = "";
                while ((lineAck = br.readLine()) != null && !lineAck.isEmpty()) {
                    Log.d("Client", "Reading response from server from port : " + remotePort + " : " + lineAck);
                    response.append(lineAck);
                    break;
                }
                Log.d("Client", "Total response from port " + remotePort + " : " + response.toString());

                os.close();
                is.close();
                socket.close();
            } catch (SocketTimeoutException e) {
                try {
                    Node node1 = activeNodes.get(genHash(Long.valueOf(remotePort) / 2 + ""));
                    node1.setActive(false);
                    Log.d("Client", "Socket for Port : " + remotePort + " timed out. Deactivated the node : " + node1);
                } catch (NoSuchAlgorithmException e1) {
                    e1.printStackTrace();
                }
                e.printStackTrace();
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
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        return 0;
    }
}