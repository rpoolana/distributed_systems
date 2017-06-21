package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MapDAO {

    private Map<String, DynamoMessage> data = new ConcurrentHashMap<String, DynamoMessage>();
    private int proposedSeqNum = 0;

    private static MapDAO mapDAO = null;

    public static MapDAO getInstance() {
        if(mapDAO == null) {
            mapDAO = new MapDAO();
        }
        return mapDAO;
    }

    public Map<String, DynamoMessage> getData() {
        return data;
    }

    public synchronized void addData(String key, DynamoMessage value) {
        Log.d("addData", "received key : "+key +" , value : "+value);

        getData().put(key, value);

        Log.d("addData", "added key : "+key +" , value : "+value);

        printAllData();
    }

    public DynamoMessage getData(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        } else {
            DynamoMessage value = data.get(key);
            Log.d("getData", "retrieving value for key : "+key +".  value found is : "+value);
            return value;
        }
    }

    public String getMessage(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        } else {
            DynamoMessage value = data.get(key);
            Log.d("getData", "retrieving value for key : "+key +".  value found is : "+value);
            return value.getMessage();
        }
    }

    public String printAllData() {
        Set<String> keys = data.keySet();
        StringBuilder db = new StringBuilder();
        db.append("printData : ");
        for (String key : keys) {
            db.append(key + "::" + data.get(key) + ",");
        }
        return db.toString();
    }
}
