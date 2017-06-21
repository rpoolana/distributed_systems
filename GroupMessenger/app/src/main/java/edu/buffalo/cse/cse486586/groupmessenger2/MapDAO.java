package edu.buffalo.cse.cse486586.groupmessenger2;

import android.util.Log;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by swamy on 2/17/17.
 */

public class MapDAO {

    private Map<String, String> data = new LinkedHashMap<String, String>();
    private int proposedSeqNum = 0;

    private synchronized int incrementSeqNum() {

        Random r = new Random();
        int random = (r.nextInt(49))+1;

        proposedSeqNum = proposedSeqNum + random;
        return proposedSeqNum;
    }

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

        Set<String> keys = getData().keySet();
        Set<Integer> keyInts = new HashSet<Integer>(keys.size());
        for(String k : keys) {
            if(Character.digit(key.charAt(0),10) >= 0) {
                keyInts.add(Integer.valueOf(k));
            }
        }

        String newKey = "";
        if(keys == null || keys.isEmpty()) {
            newKey = 0+"";
        } else {
            newKey = Collections.max(keyInts)+1+"";
        }

        getData().put(newKey, value);
        Log.d("addData", "added key : "+newKey +" , value : "+value);
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
        db.append("Current db : ");
        for (String key : keys) {
            db.append(key + "::" + data.get(key) + ",");
        }
        return db.toString();
    }

   public synchronized int getProposedSeqNum() {

       return incrementSeqNum();
    }
}
