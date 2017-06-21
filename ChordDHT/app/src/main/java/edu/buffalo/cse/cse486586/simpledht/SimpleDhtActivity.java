package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.os.StrictMode;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    private String myPort = "";
    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        if (android.os.Build.VERSION.SDK_INT > 9) {
            StrictMode.ThreadPolicy policy = new StrictMode.ThreadPolicy.Builder().permitAll().build();
            StrictMode.setThreadPolicy(policy);
        }

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        // int randomNum = new Random().nextInt((100-1)+1)+1;

        final Button lDump = (Button) findViewById(R.id.button1);
        lDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor resultCursor = getContentResolver().query(mUri,null,"@",null,null);
                Log.d("LDump", resultCursor.getCount()+"");
            }
        });

        final Button gDump = (Button) findViewById(R.id.button2);
        gDump.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Cursor resultCursor = getContentResolver().query(mUri,null,"*",null,null);
                Log.d("GDump", resultCursor.getCount()+"");
            }
        });

        /*StrictMode.setVmPolicy (new StrictMode.VmPolicy.Builder().detectAll().penaltyLog()
                .penaltyDeath().build());*/
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
