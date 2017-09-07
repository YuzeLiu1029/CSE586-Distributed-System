package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider {

    String successor = new String();  //avd number
    String predecessor = new String(); // avd number
    String myPort = new String();
    String myNode = new String();
    String myAvdNumber= new String(); //avd number
    static final int SERVER_PORT = 10000;


    //packetType :
    int requestJoin = 0;
    int ackforRequest = 1;

    int insertRequest = 2;
    int insertTo = 3;

    int querykey = 4;
    int queryAll = 5;
    int querylocal = 6;


    int deletekey = 7;
    int deleteAll = 8;
    int deletelocal = 9;

    int returnQuery = 10;
    int updateReq = 11;

    int queryTo5554 = 12;
    int deleteTo5554 = 13;

    boolean queryResultStatus = false;
    boolean ackReceivedStatus = false;
    boolean queryAllResultStatus = false;

    String[] returnQueryResultRemote = new String[2];
    String[] returnQueryResultLocal = new String[2];
    String queryAllStringResult = "";

    ArrayList<String> mylist5554 = new ArrayList<String>();








    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

//        ArrayList<String> mylist = new ArrayList<String>();
//        try{
//            mylist.add(genHash("5554"));
//            mylist.add(genHash("5556"));
//            mylist.add(genHash("5558"));
//            mylist.add(genHash("5560"));
//            mylist.add(genHash("5562"));
//            mylist.add(genHash("HAdDmHgTKmPeJHiGVZBHLpTzB58vTiqW"));
//            Collections.sort(mylist);
//
//        }catch(Exception e ){
//            e.printStackTrace();
//        }


        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length()-4);
        myPort = String.valueOf((Integer.parseInt(portStr)*2));
        myAvdNumber = getAvdNumber(myPort);
        Log.i("My avd number",myAvdNumber);
        try{
            myNode = genHash(myAvdNumber);
        } catch (NoSuchAlgorithmException e){
            Log.e("Fail","Get the hash key of node.");
        }
        if(myAvdNumber.equals("5554")){
            Log.i("My avd Number", "5554, wait for joining");
            successor = myAvdNumber;
            predecessor = myAvdNumber;
            mylist5554.add("5554");
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
                Log.i("Create Server Socket","Successful");

            } catch (Exception e){
                Log.e("OnCreate : ", "Fail to create 5554 ServerSocket");
                e.printStackTrace();

            }
            return true;

        } else {
            Log.i("My avd number", myAvdNumber);
            successor = myAvdNumber;
            predecessor = myAvdNumber;
            try{
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);
                Log.i("Create Server Socket","Successful");

            } catch (Exception e){
                Log.e("OnCreate : ", "Fail to create ServerSocket");
                e.printStackTrace();

            }
            ExchangePacket requestJoinPacket = new ExchangePacket();
            requestJoinPacket.packetType = requestJoin;
            requestJoinPacket.originalSenderAvd = myAvdNumber;
            requestJoinPacket.receiverAvd = "5554";
            Log.e("Request Join", requestJoinPacket.ToString());
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,requestJoinPacket);
            Log.i("Request Join Packet","Send");

            //}
            return true;

        }
    }

    public class ServerTask extends AsyncTask<ServerSocket,ExchangePacket,Void>{

        @Override
        protected Void doInBackground(ServerSocket... sockets){
            ServerSocket serverSocket = sockets[0];
            String receivedNode = new String();
            try {
                while (true){
                    Socket s = serverSocket.accept();
                    Log.i("Server Task","received Packet");
                    ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
                    //ExchangePacket receivedpacket = new ExchangePacket();
                    ExchangePacket receivedpacket = (ExchangePacket) ois.readObject();
                    if(receivedpacket.originalSenderAvd != null){
                        receivedNode = genHash(receivedpacket.originalSenderAvd);
                    }
                    Log.i("Server Task", receivedpacket.ToString());


                    if(receivedpacket.packetType == requestJoin){
                        Log.i("Server Task","received request Join Packet");
                        String[] position = findPosition(receivedpacket.originalSenderAvd);
                        Log.e("Position",position[0] + "," + position[1]);


                        ExchangePacket ackPacket = new ExchangePacket();
                        ackPacket.packetType = ackforRequest;
                        ackPacket.asignSuccessor = position[0];
                        ackPacket.assignPredecessor = position[1];
                        ackPacket.receiverAvd = receivedpacket.originalSenderAvd;
                        Log.e("Reply to Join", ackPacket.asignSuccessor + "," + ackPacket.asignSuccessor);
                        Log.e("Received Packet",receivedpacket.ToString());
                        Log.e("Ackpacket", ackPacket.ToString());
                        publishProgress(ackPacket);

                        ExchangePacket ackPacket_p = new ExchangePacket();
                        ackPacket_p.packetType = ackforRequest;
                        ackPacket_p.receiverAvd = position[1];
                        ackPacket_p.asignSuccessor = receivedpacket.originalSenderAvd;
                        ackPacket_p.assignPredecessor = "nochange";
                        Log.e("Position",position[0] + "," + position[1]);
                        Log.e("Reply P to Join", ackPacket_p.asignSuccessor + "," + ackPacket_p.assignPredecessor);
                        Log.e("Received Packet",receivedpacket.ToString());
                        Log.e("Ackpacket_p", ackPacket_p.ToString());
                        publishProgress(ackPacket_p);

                        ExchangePacket ackPacket_s = new ExchangePacket();
                        ackPacket_s.packetType = ackforRequest;
                        ackPacket_s.receiverAvd = position[0];
                        ackPacket_s.assignPredecessor = receivedpacket.originalSenderAvd;
                        ackPacket_s.asignSuccessor = "nochange";
                        Log.e("Position",position[0] + "," + position[1]);
                        Log.e("Reply S to Join", ackPacket_s.asignSuccessor + "," + ackPacket_s.asignSuccessor);
                        Log.e("Received Packet",receivedpacket.ToString());
                        Log.e("Ackpacket_s", ackPacket_s.ToString());
                        publishProgress(ackPacket_s);

                    } else if(receivedpacket.packetType == ackforRequest){
                        // TODO : Update my own information
                        Log.i("Server Task","received Ack Packet");
                        if(!receivedpacket.asignSuccessor.equals("nochange")){
                            successor = receivedpacket.asignSuccessor;
                        }
                        if(!receivedpacket.assignPredecessor.equals("nochange")){
                            predecessor = receivedpacket.assignPredecessor;
                        }
                        Log.e("Join Successfully", myAvdNumber);
                        Log.e("Successor", successor);
                        Log.e("Predecessor", predecessor);

                    } else if(receivedpacket.packetType == insertRequest){
                        String receivedKeyId = genHash(receivedpacket.exchangeKey);
                        if(isLocal(receivedpacket.exchangeKey)){
                            insertLocalFunc(receivedpacket.exchangeKey,receivedpacket.contentValue);
                        } else {

                            receivedpacket.receiverAvd = successor;
                            publishProgress(receivedpacket);
                        }
                    } else if(receivedpacket.packetType == querykey){
                        // TODO : query local or send to successor
                        if(isLocal(receivedpacket.exchangeKey)){
                            String[] queryReturn = queryRemoteFunc(receivedpacket.exchangeKey);
                            ExchangePacket queryResultPkt = new ExchangePacket();
                            queryResultPkt.receiverAvd = receivedpacket.originalSenderAvd;
                            queryResultPkt.packetType = returnQuery;
                            queryResultPkt.exchangeKey = queryReturn[0];
                            queryResultPkt.contentValue = queryReturn[1];
                            queryResultPkt.originalSenderAvd = myAvdNumber;
                            publishProgress(queryResultPkt);
                        } else {
                            receivedpacket.receiverAvd = successor;
                            publishProgress(receivedpacket);
                        }
                    } else if(receivedpacket.packetType == queryAll){
                        // TODO : query local, send to successor if successor is not "5554"
                        if(receivedpacket.originalSenderAvd.equals(myAvdNumber)){
                            Log.e("Query All","Stop");
                            queryAllStringResult = receivedpacket.contentValue;
                            queryAllResultStatus = true;
                        }else {
                            Log.e("Server Query All", "Forward Next");
                            String[] filenameList = getContext().fileList();
                            StringBuilder str = new StringBuilder();
                            if(!receivedpacket.contentValue.isEmpty()){
                                str.append(receivedpacket.contentValue);
                                str.append(",");
                            }
                            for(int i = 0; i< filenameList.length; i++){
                                String filename = filenameList[i];
                                returnQueryResultLocal = queryLocalFunc(filename);
                                String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
                                str.append(returnQueryResultLocal[0]);
                                str.append(",");
                                str.append(returnQueryResultLocal[1]);
                                str.append(",");
                            }
//                            returnQueryResultLocal = queryLocalFunc(filenameList[filenameList.length -1]);
//                            String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
//                            str.append(returnQueryResultLocal[0]);
//                            str.append(",");
//                            str.append(returnQueryResultLocal[1]);



                            receivedpacket.receiverAvd = successor;
                            receivedpacket.contentValue = str.toString();
                            publishProgress(receivedpacket);
                        }
                    } else if(receivedpacket.packetType == deletekey){
                        // TODO : delete local or send to successor
                        //String keyHashId = genHash(receivedpacket.exchangeKey);
                        if(isLocal(receivedpacket.exchangeKey)){
                            deleteLocalFunc(receivedpacket.exchangeKey);
                        } else {
                            receivedpacket.receiverAvd = successor;
                            publishProgress(receivedpacket);
                        }

                    }  else if(receivedpacket.packetType == deleteAll){
                        // TODO : delete local and send to successor
                        deleteLocalAllFunc();
                        if(!successor.equals(receivedpacket.originalSenderAvd)){
                            receivedpacket.receiverAvd = successor;
                            publishProgress(receivedpacket);
                        }
                    } else  if(receivedpacket.packetType == returnQuery){
                        // TODO : received returnQuery Packet, query
                        returnQueryResultRemote[0] = receivedpacket.exchangeKey;
                        returnQueryResultRemote[1] = receivedpacket.contentValue;
                        queryResultStatus = true;
                    } else if(receivedpacket.packetType == updateReq){
                        Log.i("Server","Received update Request");
                        successor = receivedpacket.asignSuccessor;
                    } else {
                        Log.i("Server Task","Unknown Packet Type");
                    }
                }
            } catch (Exception e){
                Log.e("Server", "Problem while recieving: "+e.getMessage());
                e.printStackTrace();

            }
            return null;
        }

        protected void onProgressUpdate(ExchangePacket... packets){
            //Log.i(myAvdNumber,"Sent Packet");
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,packets);
            //new ClientTask().execute(packets);
        }
    }

    public class ClientTask extends AsyncTask<ExchangePacket,Void,Void>{
        @Override
        protected Void doInBackground(ExchangePacket... outGoingPacket){
            try{
                String remotePort = getIPport(outGoingPacket[0].receiverAvd);
                Log.i("Client Task",remotePort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                Log.i("Client Task","Socket Created Successfully");
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(outGoingPacket[0]);
                //Log.e("Packet : ", outGoingPacket[0].ToString());
                oos.flush();
                oos.close();
                //Log.i(myAvdNumber,"Packet sent successfully");
                //socket.close();
            } catch (Exception e){
                Log.e("ClientTask","Packet sent failed");
                e.printStackTrace();
            }
            return null;
        }
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("@")){
            // TODO : delete all local file
            deleteLocalAllFunc();

        } else  if (selection.equals("*")){
            // TODO : delete select key pair
            deleteLocalAllFunc();
            if(successor != null){
                ExchangePacket sendNewpacket = new ExchangePacket();
                sendNewpacket.packetType = deleteAll;
                sendNewpacket.originalSenderAvd = myAvdNumber;
                sendNewpacket.receiverAvd = successor;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,sendNewpacket);
//
            }
        } else {
            try {
                if(isLocal(selection)){
                    deleteLocalFunc(selection);
                } else {

                    ExchangePacket sendNewpacket =  new ExchangePacket();
                    sendNewpacket.packetType = deletekey;
                    sendNewpacket.receiverAvd = successor;
                    sendNewpacket.originalSenderAvd = myAvdNumber;
                    sendNewpacket.exchangeKey = selection;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,sendNewpacket);
                }

            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return 0;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO : insert the key pair to local file
        String keyV = values.get("key").toString();
        String valueofK = values.get("value").toString();
        Log.i("Insert",keyV);
        Log.i("Insert",valueofK);

        try {

            if(isLocal(keyV)){
                insertLocalFunc(keyV,valueofK);
            } else {
                ExchangePacket newSendPacket = new ExchangePacket();
                newSendPacket.packetType = insertRequest;
                newSendPacket.originalSenderAvd = myAvdNumber;
                newSendPacket.receiverAvd = successor;
                newSendPacket.exchangeKey = keyV;
                newSendPacket.contentValue = valueofK;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,newSendPacket);

            }
        } catch (Exception e){

        }
        return null;
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Log.i("Query", selection);
        // TODO Auto-generated method stub
        if(selection.equals("@")){
            // TODO : Query all the local key-pair
            String[] filenameList = getContext().fileList();
            String [] accessTable = {"key","value"};
            MatrixCursor cursor = new MatrixCursor(accessTable);

            for(int i = 0; i< filenameList.length; i++){
                String filename = filenameList[i];
                returnQueryResultLocal = queryLocalFunc(filename);
                String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
                cursor.addRow(newRowLocal);
            }
            return cursor;
        } else if(selection.equals("*")){
            // TODO : Query the key-pair and send to successor
            queryAllResultStatus = false;
            String[] filenameList = getContext().fileList();
            String [] accessTable = {"key","value"};
            MatrixCursor cursor = new MatrixCursor(accessTable);

            for(int i = 0; i< filenameList.length; i++){
                String filename = filenameList[i];
                returnQueryResultLocal = queryLocalFunc(filename);
                String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
                cursor.addRow(newRowLocal);
            }

            ExchangePacket newSenderPacket = new ExchangePacket();
            newSenderPacket.packetType = queryAll;
            newSenderPacket.originalSenderAvd = myAvdNumber;
            newSenderPacket.contentValue = "";

            Log.e("qa","1");

            if(!successor.equals(myAvdNumber)){
                newSenderPacket.receiverAvd = successor;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,newSenderPacket);
                Log.e("qa","2");
            } else {
                Log.e("qa","3");
                return cursor;
            }

            Log.e("qa","4");
            while(queryAllResultStatus == false){
                Log.e("Wait","query all");
                try{
                    Thread.sleep(5000);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }

            Log.e("String",queryAllStringResult);

            String[] tokens = queryAllStringResult.split(",");
            ArrayList<String> temp = new ArrayList<String>();
            for(int i = 0; i < tokens.length; i++){
                if(!tokens[i].isEmpty()){
                    temp.add(tokens[i]);
                }
            }

            for(int i= 0; i < temp.size(); i +=2 ){
                cursor.addRow(new String[]{temp.get(i),temp.get(i+1)});
            }



//            if(successor != null){
//                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,newSenderPacket);
//            }
            return cursor;
        } else {
            try{
                if(isLocal(selection)){
                    returnQueryResultLocal = queryLocalFunc(selection);
                    String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
                    String[] accessTable = {"key","value"};
                    MatrixCursor cursor = new MatrixCursor(accessTable);
                    cursor.addRow(newRowLocal);
                    return cursor;
                } else {
                    ExchangePacket queryNewPacket = new ExchangePacket();
                    queryNewPacket.packetType = querykey;
                    queryNewPacket.receiverAvd = successor;
                    queryNewPacket.exchangeKey = selection;
                    queryNewPacket.originalSenderAvd = myAvdNumber;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,queryNewPacket);
                    while(queryResultStatus == false){}
                    String[] newRow = {returnQueryResultRemote[0],returnQueryResultRemote[1]};
                    String[] accessTable = {"key","value"};
                    MatrixCursor cursor = new MatrixCursor(accessTable);
                    cursor.addRow(newRow);
                    queryResultStatus = false;
                    return cursor;
                }
            } catch (Exception e){
                e.printStackTrace();

            }

        }
        return null;
    }

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

    public String getAvdNumber(String portNumber){
        String avdNumber;
        if(portNumber.equals("11108")){
            avdNumber = "5554";
        } else if(portNumber.equals("11112")){
            avdNumber = "5556";
        } else if(portNumber.equals("11116")){
            avdNumber = "5558";
        } else if(portNumber.equals("11120")){
            avdNumber = "5560";
        } else if(portNumber.equals("11124")){
            avdNumber = "5562";
        } else {
            avdNumber = null;
        }
        return avdNumber;
    }

    public String getIPport(String avdNumber){
        String portNumber;
        if(avdNumber.equals("5554")){
            portNumber = "11108";
        } else if(avdNumber.equals("5556")){
            portNumber = "11112";
        } else if(avdNumber.equals("5558")){
            portNumber = "11116";
        } else if(avdNumber.equals("5560")){
            portNumber = "11120";
        } else if(avdNumber.equals("5562")){
            portNumber = "11124";
        } else {
            portNumber = null;
        }
        return portNumber;
    }

    public void insertLocalFunc(String key, String value){
        try{
            //String keyId = genHash(key);
            String keyId = key;
            FileOutputStream fos = getContext().openFileOutput(keyId,Context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
        } catch (Exception e){
            Log.e("Insert Local File List","Fail to write to the file");
        }

    }

    public String[] queryLocalFunc(String key){

        try{
            String[] queryResultReturn = new String[2];
            Log.i("Query Local Func", key);
            //String filename = genHash(key);
            String filename = key;
            StringBuffer content = new StringBuffer();

            FileInputStream fIn = getContext().openFileInput(filename);
            InputStreamReader isr = new InputStreamReader(fIn);
            BufferedReader bufferedReader = new BufferedReader(isr);

            String readString = bufferedReader.readLine();
            while (readString != null){
                content.append(readString);
                readString = bufferedReader.readLine();
            }
            isr.close();

            Log.i("Query Local Func",key);
            Log.i("Query Local Func",content.toString());

            String contentVal = content.toString();
            queryResultReturn[0] = key;
            queryResultReturn[1] = contentVal;
            Log.i("Return Query Local Func",queryResultReturn[0]);
            Log.i("Return Query Local Func",queryResultReturn[1]);
            return queryResultReturn;


        } catch (Exception e){
            Log.i("Query Local Func","query failed");
            e.printStackTrace();

        }
        Log.v("query", key);
        return null;
    }

    public String[] queryRemoteFunc(String key){
        try{
            //String filename = genHash(key);
            String filename = key;
            StringBuffer content = new StringBuffer();

            FileInputStream fIn = getContext().openFileInput(filename);
            InputStreamReader isr = new InputStreamReader(fIn);
            BufferedReader bufferedReader = new BufferedReader(isr);

            String readString = bufferedReader.readLine();
            while (readString != null){
                content.append(readString);
                readString = bufferedReader.readLine();
            }
            isr.close();

            String contentVal = content.toString();
            String[] newRow = {key,contentVal};

            return newRow;

        } catch (Exception e){

        }
        return null;
    }

    public Cursor queryLocalAllFunc(){
        try{
            String[] filenameList = getContext().fileList();
            String [] accessTable = {"key","value"};
            MatrixCursor cursor = new MatrixCursor(accessTable);
            for(int i = 0; i < filenameList.length; i++){
                String filename = filenameList[i];
                StringBuffer content = new StringBuffer();

                FileInputStream fIn = getContext().openFileInput(filename);
                InputStreamReader isr = new InputStreamReader(fIn);
                BufferedReader bufferedReader = new BufferedReader(isr);

                String readString = bufferedReader.readLine();
                while (readString != null){
                    content.append(readString);
                    readString = bufferedReader.readLine();
                }
                isr.close();
                String contentVal = content.toString();
                String[] newRow = {filenameList[i],contentVal};
                cursor.addRow(newRow);
            }

            return cursor;
        } catch (Exception e){

        }
        return null;
    }

    public void deleteLocalFunc(String key){
        try{
            //String keyId = genHash(key);
            getContext().deleteFile(key);
        } catch (Exception e){

        }
    }

    public void deleteLocalAllFunc(){
        try{
            String[] localFileList = getContext().fileList();
            for(int i =0; i < localFileList.length;i++){
                getContext().deleteFile(localFileList[i]);
            }

        } catch (Exception e){

        }

    }

    public String[] findPosition(String avd){
        String s = "";
        String p = "";
        String[] returnSP = new String[2];

        if(!mylist5554.contains(avd)){
            mylist5554.add(avd);
        }

        Collections.sort(mylist5554, new Comparator<String>() {
            @Override
            public int compare(String lhs, String rhs) {
                int flag;
                try{
                    flag = genHash(lhs).compareTo(genHash(rhs));
                    return flag;
                } catch(Exception e){
                    Log.e("kkk","jjjj");
                }
                return 0;
            }
        });
        for(int i = 0; i < mylist5554.size(); i++){
            if(mylist5554.get(i).equals(avd)){
                if(i == 0){
                    s = mylist5554.get(1);
                    p = mylist5554.get(mylist5554.size()-1);

                } else if( i == mylist5554.size() - 1){
                    s = mylist5554.get(0);
                    p = mylist5554.get(mylist5554.size()-2);
                } else {
                    s = mylist5554.get(i+1);
                    p = mylist5554.get(i - 1);
                }
                break;
            }

        }

        returnSP[0] = s;
        returnSP[1] = p;

        return returnSP;
    }

    public boolean isLocal(String keyV){
        String keyId = "";
        String pId = "";
        String sId = "";
        String mId = "";
        try {
            keyId = genHash(keyV);
            pId = genHash(predecessor);
            sId = genHash(successor);
            mId = genHash(myAvdNumber);

        } catch (Exception e){
            e.printStackTrace();
        }

        if( (keyId.compareTo(pId) > 0 && keyId.compareTo(mId) <= 0 ) ||
                (pId.compareTo(mId) > 0 && keyId.compareTo(pId) > 0 ) ||
                (pId.compareTo(mId) > 0 && keyId.compareTo(mId) <= 0) ||
                (predecessor.equals(myAvdNumber) && predecessor.equals(successor))){
            return true;
        } else {
            return false;

        }

    }

}

