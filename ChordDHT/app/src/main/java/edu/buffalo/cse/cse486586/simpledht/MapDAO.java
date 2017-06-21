package edu.buffalo.cse.cse486586.simpledht;

import android.util.Log;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by swamy on 2/17/17.
 */

public class MapDAO {

    private Map<String, String> data = new LinkedHashMap<String, String>();
    private int proposedSeqNum = 0;

    private static MapDAO mapDAO = null;

    public static MapDAO getInstance() {
        if(mapDAO == null) {
            mapDAO = new MapDAO();
        }
        return mapDAO;
    }

    public Map<String, String> getData() {
        return data;
    }

    public synchronized void addData(String key, String value) {
        Log.d("addData", "received key : "+key +" , value : "+value);

        getData().put(key, value);

        Log.d("addData", "added key : "+key +" , value : "+value);

        printAllData();
    }

    public String getData(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        } else {
            String value = data.get(key);
            Log.d("getData", "retrieving value for key : "+key +".  value found is : "+value);
            return value;
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
