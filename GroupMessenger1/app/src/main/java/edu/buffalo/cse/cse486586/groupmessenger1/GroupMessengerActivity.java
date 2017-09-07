package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    /*
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    */
    static final int SERVER_PORT = 10000;
    int msgCount = 0;
    static final String[] REMOTE_PORT = {"11108","11112","11116","11120","11124"};
    //private final ContentResolver myContentResolver;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        final ContentResolver mContentResolver;
        final Uri mUri;
        final ContentValues[] mContentValues;


        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length()-4);
        final String myPort = String.valueOf((Integer.parseInt(portStr)*2));

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
        } catch (IOException e){
            Log.e(TAG,"Can't create a ServerSocket");
            return;
        }


        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {
                // Perform action on click
                String msg = editText.getText().toString() + "\n";


                //Context context = getBaseContext();
                //ContentValues cv = new ContentValues();
                //cv.put("key", Integer.toString(msgCount));
                //cv.put("value", msg);

                //ContentResolver cr = getContentResolver();
                //final Uri mUri;
                //mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
                //cr.insert(mUri, cv);

                editText.setText("");
                //TextView localTextView = (TextView) findViewById(R.id.local_text_display);
                TextView localTextView = (TextView)findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                //TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                //remoteTextView.append("\n");
                localTextView.append("\n");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private class ServerTask extends AsyncTask<ServerSocket,String,Void>{
        @Override
        protected  Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            try {
                while(true){
                    Socket s = serverSocket.accept();
                    DataInputStream dis = new DataInputStream(s.getInputStream());
                    String message = dis.readUTF();
                    publishProgress(message);
                    System.out.println("message received from client = " + message);
                    s.close();


                    ContentValues cv2 = new ContentValues();
                    cv2.put("key", Integer.toString(msgCount));
                    cv2.put("value", message);

                    ContentResolver cr = getContentResolver();
                    final Uri mUri;
                    mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger1.provider");
                    cr.insert(mUri, cv2);
                    msgCount = msgCount + 1;
                    }
            } catch (IOException e) {
                Log.e(TAG,"Fail to accept a connection");
            }
            return null;
        }

        protected  void onProgressUpdate(String...strings){

            String filename = "SimpleMessengerOutput";
            String strReceived = strings[0].trim();
            String string = strReceived + "\n";
            FileOutputStream outputStream;

            try {
                outputStream = openFileOutput(filename, Context.MODE_PRIVATE);
                outputStream.write(string.getBytes());
                outputStream.close();
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }

            return;

        }

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                for(int i =0;i<5;i++){
                    String remotePort = REMOTE_PORT[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String msgToSend = msgs[0];
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();
                    socket.close();
                }
                //String remotePort = REMOTE_PORT0;
                //if (msgs[1].equals(REMOTE_PORT0)){
                //    remotePort = REMOTE_PORT1;}
                /*
                } else if(msgs[1].equals(REMOTE_PORT1)){
                    remotePort = REMOTE_PORT2;
                } else if(msgs[1].equals((REMOTE_PORT2))){
                    remotePort = REMOTE_PORT3;
                } else if(msgs[1].equals((REMOTE_PORT3))){
                    remotePort = REMOTE_PORT4;
                }
                */
                //Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        //Integer.parseInt(remotePort));

                //String msgToSend = msgs[0];
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                /***********************************************************************************/
                //From here is the added code by Yuze//
                //while(true) {
                //DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                //dout.writeUTF(msgToSend);
                //dout.flush();
                //}
                /***********************************************************************************/

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
