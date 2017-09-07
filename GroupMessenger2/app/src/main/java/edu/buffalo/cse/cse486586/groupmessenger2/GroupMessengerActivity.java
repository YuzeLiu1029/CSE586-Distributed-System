package edu.buffalo.cse.cse486586.groupmessenger2;

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
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    int msgCount = 0;
    static final String[] REMOTE_PORT = {"11108","11112","11116","11120","11124"};
    static String emulatorPort = null;


    /********************************** Begin New Added **********************************/
    int countMsgID = -1;
    int countSequence = -1;
    int agreedSequence = -1;
//    PriorityQueue<Message> deliverQueue = new PriorityQueue<Message>();
    PriorityBlockingQueue<Message> deliverQueue = new PriorityBlockingQueue<Message>();
    /********************************** End New Added ***********************************/


    @Override
    protected void onCreate(Bundle savedInstanceState) {

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length()-4);
        final String myPort = String.valueOf((Integer.parseInt(portStr)*2));
        emulatorPort = myPort;

        /********************************** Begin New Added **********************************/
//        Message msg1 = new Message (2,4,11108,9,11112,1,"FWflB5yJYDntjHwctoZHAOsh9Ii9DKgZ");
//        Message msg2 = new Message (1,4,11112,9,11108,0,"u8hnhfz3SKiKmIY4CEiWcWhDrbd87qkI");
//        deliverQueue.add(msg1);
//        deliverQueue.add(msg2);
//        System.out.println(deliverQueue.peek().msg);
        //Log.i("My port",myPort);
        /********************************** End New Added **********************************/

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);

        } catch (IOException e){
            Log.e(TAG,"Can't create a ServerSocket");
            return;
        }

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));


        final EditText editText = (EditText) findViewById(R.id.editText1);

        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            public void onClick(View v) {

                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                TextView localTextView = (TextView)findViewById(R.id.textView1);
                localTextView.append("\t" + msg); // This is one way to display a string.
                localTextView.append("\n");

                /********************************** Begin New Added **********************************/
                //Get the message from the TextView, edit the message, add message id, processor id, msg and deliverable status
                countMsgID++;
                Message newMsg = new Message(msg);
                newMsg.messageType = 0;
                newMsg.deliverableStatus = 1;
                newMsg.messageId = countMsgID;
                newMsg.processorID = Integer.valueOf(myPort);
                newMsg.sequence = 0;
                newMsg.anotherProcessorID = 0;

                String newMessageToSend = newMsg.MessageToStringMsg();
                /**TO DO ADD THE MESSAGE IN THE QUEUE**/
                deliverQueue.add(newMsg);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, newMessageToSend);
                //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, newMessageToSend);
                /********************************** End New Added ***********************************/

            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
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
                    dis.close();
                    s.close();
                    /********************************** Begin New Added **********************************/
                    Message receivedMsg = parseMessage(message);
                    int receivedMsgType = receivedMsg.messageType;
                    switch (receivedMsgType){
                        case 0 : //new Message
                            //Log.i("New Message", "Received");
                            if(receivedMsg.processorID == Integer.valueOf(emulatorPort)){
                                break;
                            }
                            countSequence = Math.max(countSequence,agreedSequence) + 1;
                            //countSequence++;
                            receivedMsg.messageType = 1; //Change the msg type to proposedMsg
                            receivedMsg.anotherProcessorID = Integer.valueOf(emulatorPort);
                            receivedMsg.sequence = countSequence;
                            publishProgress(receivedMsg.MessageToStringMsg());
                            //sendMessagetoALL(receivedMsg.toString());
                            deliverQueue.add(receivedMsg);
                            break;
                        case 1 : //proposed Message
                            Log.i("Proposed Msg Received", receivedMsg.sequence + "," + receivedMsg.anotherProcessorID + "," + receivedMsg.msg);

//                            for(Iterator<Message> it = deliverQueue.iterator(); it.hasNext();){
//                                Message m;
//                                m = it.next();
//                                if (m.equals(receivedMsg)){
//                                    m.getProposedSender(receivedMsg);
//
//                                    //receivedMsg.getProposedSender(receivedMsg);
//                                    if(m.getHighestSeq(receivedMsg)){
//                                        m.sequence = receivedMsg.sequence;
//                                        m.anotherProcessorID= receivedMsg.anotherProcessorID;
//                                    }
//
////                                    it.remove();
////                                    deliverQueue.add(m);
//
//                                    if(m.checkArray()){
//                                        Log.i("ArrayCheck", "1");
//                                        m.messageType = 2; //Agreement
//                                        publishProgress(m.MessageToStringMsg());
//                                        //sendMessageBack(m.MessageToStringMsg(),m.processorID);
//                                        Log.i("Agreement Message", "Sent");
//                                    } else {
//                                        Log.i("ArrayCheck", "0");
//                                        Log.i("Array",m.proposedArray[0]+"," + m.proposedArray[1] +"," + m.proposedArray[2]+"," + m.proposedArray[3]+"," + m.proposedArray[4]);
//                                    }
//
//                                    it.remove();
//                                    deliverQueue.add(m);
//                                    break;
//                                }
//                            }
                            Log.i("Queue Size", String.valueOf(deliverQueue.size()));

                            for(Message m : deliverQueue){
                                if(m.equals(receivedMsg)){
                                    m.getProposedSender(receivedMsg);
                                    if(m.getHighestSeq(receivedMsg)){
                                        m.replace(receivedMsg);
                                    }

                                    if(m.checkArray()){
                                        Log.i("ArrayCheck", "1");
                                        m.messageType = 2; //Agreement
                                        publishProgress(m.MessageToStringMsg());
                                        //sendMessageBack(m.MessageToStringMsg(),m.processorID);
                                        Log.i("Agreement Message", "Sent");
                                    } else {
                                        Log.i("ArrayCheck", "0");
                                        Log.i("Array",m.proposedArray[0]+"," + m.proposedArray[1] +"," + m.proposedArray[2]+"," + m.proposedArray[3]+"," + m.proposedArray[4]);
                                    }
                                    break;
                                }
                            }
                            break;

                        case 2 : //Agreement Message
                            Log.i("Agreement Msg Received", receivedMsg.sequence + "," + receivedMsg.anotherProcessorID+","+receivedMsg.msg);
                            agreedSequence = Math.max(agreedSequence,receivedMsg.sequence);
//                            for(Message m : deliverQueue){
//                                if(m.equals(receivedMsg)){
//                                    m.anotherProcessorID = receivedMsg.anotherProcessorID;
//                                    m.sequence = receivedMsg.sequence;
//                                    m.deliverableStatus = 2;
//                                    break;
//                                }
//                            }


                            for(Iterator<Message> it = deliverQueue.iterator(); it.hasNext();){
                                Message m;
                                m = it.next();
                                if (m.equals(receivedMsg)){
                                    it.remove();
                                    break;
                                }
                            }

                            receivedMsg.deliverableStatus = 2;
                            deliverQueue.add(receivedMsg);




                            if(deliverQueue.size() > 1){
                                Log.i("Current queue size",deliverQueue.size() + ", peek : "+ deliverQueue.peek().sequence+","+ deliverQueue.peek().anotherProcessorID+","+ deliverQueue.peek().deliverableStatus+","+ deliverQueue.peek().msg);
                            }

                            if(deliverQueue.size() > 1){
                                //Log.i("Current queue size",deliverQueue.size() + ", peek : "+ deliverQueue.peek().sequence+","+ deliverQueue.peek().anotherProcessorID+","+ deliverQueue.peek().deliverableStatus+","+ deliverQueue.peek().msg);
                                printQueue(deliverQueue);
                            }

//                            while(deliverQueue.peek() != null && deliverQueue.peek().deliverableStatus == 1){
//                                Log.i(String.valueOf(deliverQueue.peek().sequence),String.valueOf(deliverQueue.peek().anotherProcessorID));
//                                ContentValues cv2 = new ContentValues();
//                                cv2.put("key", Integer.toString(msgCount));
//                                cv2.put("value", deliverQueue.peek().msg);
//                                ContentResolver cr = getContentResolver();
//                                final Uri mUri;
//                                mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
//                                cr.insert(mUri, cv2);
//                                msgCount = msgCount + 1;
//                                deliverQueue.remove();
//                            }

                            while(!deliverQueue.isEmpty() ){
                                if(deliverQueue.peek().deliverableStatus == 2){
                                    Log.i(String.valueOf(deliverQueue.peek().sequence),String.valueOf(deliverQueue.peek().anotherProcessorID));
                                    ContentValues cv2 = new ContentValues();
                                    cv2.put("key", Integer.toString(msgCount));
                                    cv2.put("value", deliverQueue.peek().msg);
                                    ContentResolver cr = getContentResolver();
                                    final Uri mUri;
                                    mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
                                    cr.insert(mUri, cv2);
                                    msgCount = msgCount + 1;
                                    deliverQueue.remove();
                                } else {
                                    break;
                                }

                            }

                            if(deliverQueue.size() > 0){
                                Log.i("Current queue size",deliverQueue.size() + ", peek after remove : "+ deliverQueue.peek().sequence+","+ deliverQueue.peek().anotherProcessorID+","+ deliverQueue.peek().deliverableStatus+","+ deliverQueue.peek().msg);
                            }
                            break;
                        default:
                            Log.i("Received","Unknown Message Type");

                    }
                    /********************************** End New Added ************************************/
                }
            } catch (IOException e) {
                Log.e(TAG,"Fail to accept a connection");
            }

            return null;
        }

        protected void onProgressUpdate(String...msgs){
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0]);
            //new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msgs[0]);

        }

    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Message recMessage = parseMessage(msgs[0]);

            try{
                switch (recMessage.messageType){
                    case 0:
                    case 2:
                        sendMessagetoALL(msgs[0]);
                        break;
                    case 1:
                        sendMessageBack(msgs[0],recMessage.processorID);
                        break;
//                    case 2:
//                        sendMessagetoALL(msgs[0]);
//                        break;
                    default:
                }
            } catch (Exception e){
                Log.e(TAG, "Unknown Exception: ", e);
            }

            return null;
        }


    }
    /******************************************* Begin New Added *******************************************/
//    public void verfyMsg(Message msgToBeVerified){
////        Log.i("Message Content", msgToBeVerified.msg);
////        Log.i("Message ID", String.valueOf(msgToBeVerified.messageId));
////        Log.i("Message ProID", String.valueOf(msgToBeVerified.processorID));
////        Log.i("Message S_ID", String.valueOf(msgToBeVerified.sequence));
////        Log.i("Message AnotherPID", String.valueOf(msgToBeVerified.anotherProcessorID));
//        Log.i("Message DStatus", String.valueOf(msgToBeVerified.deliverableStatus));
//    }
    /******************************************** End New Added ********************************************/

    /******************************************* Begin New Added *******************************************/
    public Message parseMessage(String MessagebeParsed){
        Message parsedMsg = new Message(null);
        String[] splited = MessagebeParsed.split(",");
        parsedMsg.messageType = Integer.valueOf(splited[0]);
        parsedMsg.messageId = Integer.valueOf(splited[1]);
        parsedMsg.processorID = Integer.valueOf(splited[2]);
        parsedMsg.sequence = Integer.valueOf(splited[3]);
        parsedMsg.anotherProcessorID = Integer.valueOf(splited[4]);
        parsedMsg.deliverableStatus = Integer.valueOf(splited[5]);
        parsedMsg.msg = splited[6];
        parsedMsg.proposedArray[0] = Integer.valueOf(splited[7]);
        parsedMsg.proposedArray[1] = Integer.valueOf(splited[8]);
        parsedMsg.proposedArray[2] = Integer.valueOf(splited[9]);
        parsedMsg.proposedArray[3] = Integer.valueOf(splited[10]);
        parsedMsg.proposedArray[4] = Integer.valueOf(splited[11]);
//        parsedMsg.messageId = Integer.valueOf(splited[0]);
//        parsedMsg.processorID = Integer.valueOf(splited[1]);
//        parsedMsg.sequence = Integer.valueOf(splited[2]);
//        parsedMsg.anotherProcessorID = Integer.valueOf(splited[3]);
//        parsedMsg.deliverableStatus = Integer.valueOf(splited[4]);
//        parsedMsg.msg = splited[5];
//        parsedMsg.proposedArray[0] = Integer.valueOf(splited[6]);
//        parsedMsg.proposedArray[1] = Integer.valueOf(splited[7]);
//        parsedMsg.proposedArray[2] = Integer.valueOf(splited[8]);
//        parsedMsg.proposedArray[3] = Integer.valueOf(splited[9]);
//        parsedMsg.proposedArray[4] = Integer.valueOf(splited[10]);
        return  parsedMsg;

    }
    /******************************************** End New Added ********************************************/


    /******************************************* Begin New Added *******************************************/
    public boolean msgIntheQueue(Message msg, PriorityQueue<Message> msgQueue){
        for(Message m : msgQueue){
            if(m.equals(msg)){
                return true;
            }
        }
        return false;
    }
    /******************************************** End New Added ********************************************/

    /******************************************* Begin New Added *******************************************/
    public int msgIndexIntheQueue(Message msg, PriorityQueue<Message> msgQueue){
        int indexTemp = 0;
        for(Message m : msgQueue){
            if(m.equals(msg)){
                return indexTemp;
            }
            indexTemp ++;
        }
        return  0;
    }
    /******************************************** End New Added ********************************************/

    /******************************************* Begin New Added *******************************************/
    public void sendMessageBack(String msgBack, int port){
        Log.i("Proposed Message", "Sent successfully");
        try{
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10,0,2,2}),port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeUTF(msgBack);
            out.flush();
            out.close();
            socket.close();

        } catch (Exception e){
            Log.e(TAG, "ClientTask UnknownHostException");
        }
    }
    /******************************************** End New Added ********************************************/

    /******************************************* Begin New Added *******************************************/
    public void sendMessagetoALL(String msgBack){
        Message recMessage = parseMessage(msgBack);
        try {
            for (int i = 0; i < 5; i++) {
                String remotePort = REMOTE_PORT[i];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    String msgToSend = msgBack;
                    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());
                    dout.writeUTF(msgToSend);
                    dout.flush();
                    dout.close();
                    socket.close();
                //}
            }
        }catch(UnknownHostException e){
            Log.e(TAG, "ClientTask UnknownHostException");
        }catch(IOException e){
            Log.e(TAG, "ClientTask socket IOException");
        }
    }
    /******************************************** End New Added ********************************************/

    /******************************************* Begin New Added *******************************************/
    public void printQueue(PriorityBlockingQueue<Message> printQueue){
        int count = 0;
        for(Message m : printQueue){
            Log.i(String.valueOf(count), m.messageType + "," + m.messageId + ","+m.processorID + ","+ m.sequence + "," + m.anotherProcessorID + "," + m.deliverableStatus + ","+ m.msg);
            count++;
        }
    }
    /******************************************** End New Added ********************************************/


}


//                    if(msgIntheQueue(receivedMsg,deliverQueue)) {
//                        if (receivedMsg.processorID == Integer.valueOf(emulatorPort)) {
//                            for (Message m : deliverQueue) {
//                                if (m.equals(receivedMsg)) {
//                                    if (m.getHighestSeq(receivedMsg)) {
//                                        //m.getProposedSender(); /*** new ****/
//                                        m.replace(receivedMsg);
//                                    }
//                                    int tempIndex = receivedMsg.indexInPArray();
//                                    if(m.proposedArray[tempIndex] == 0){
//                                        m.proposedArray[tempIndex] = 1;
//                                    }
//                                    if (m.checkArray()) {
//                                        /**SEND TO ALL AVD**/
//                                        Log.i(" I determine " , String.valueOf(m.sequence) +","+ String.valueOf(m.anotherProcessorID));
//                                        sendMessagetoALL(m.MessageToStringMsg());
//                                        m.deliverableStatus = 1;
//                                    }
//                                    break;
//                                }
//                            }
//
//                        } else if (receivedMsg.processorID != Integer.valueOf(emulatorPort)) {
//                            if (countSequence < receivedMsg.sequence) {
//                                countSequence = receivedMsg.sequence;
//                            }
//
//                            for (Message m : deliverQueue) {
//                                if (m.equals(receivedMsg)) {
//                                    m.replace(receivedMsg);
//                                    m.deliverableStatus = 1;
//                                    break;
//                                }
//                            }
//                        }
//                        while(deliverQueue.peek() != null && deliverQueue.peek().deliverableStatus == 1){
//                            Log.i(String.valueOf(deliverQueue.peek().sequence),String.valueOf(deliverQueue.peek().anotherProcessorID));
//                            ContentValues cv2 = new ContentValues();
//                            cv2.put("key", Integer.toString(msgCount));
//                            cv2.put("value", deliverQueue.peek().msg);
//                            ContentResolver cr = getContentResolver();
//                            final Uri mUri;
//                            mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider"); /******/
//                            cr.insert(mUri, cv2);
//                            msgCount = msgCount + 1;
//                            deliverQueue.remove();
//                            //Log.i(String.valueOf(deliverQueue.peek().sequence),String.valueOf(deliverQueue.peek().anotherProcessorID));
//                        }
//
//                    } else { //message is not in the queue
//                        countSequence ++;
//                        receivedMsg.sequence = countSequence;
//                        receivedMsg.anotherProcessorID = Integer.valueOf(emulatorPort);
//                        deliverQueue.add(receivedMsg);
//                        sendMessageBack(receivedMsg.MessageToStringMsg(),receivedMsg.processorID);
//                        /**SEND TO ONE AVD**/
//                    }
