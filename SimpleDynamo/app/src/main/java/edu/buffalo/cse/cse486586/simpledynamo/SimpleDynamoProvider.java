package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


import org.json.JSONException;
import org.json.JSONObject;
import java.io.PrintWriter;



public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static final int SERVER_PORT = 10000;
	private static final String[] DynamoRing = {"5562","5556","5554","5558","5560"};
	String MYPORT = "";
	String MYAVD = "";

	private static final String INSERT = "INSERT";
	private static final String INSERTLOCAL = "INSERTLOCAL";
	private static final String QUERY = "QUERY";
	private static final String QUERYALL = "QUERYALL";
	private static final String QUERYLOCAL = "QUERYLOCAL";
	private static final String DELETE = "DELETE";
	private static final String DELETEALL = "DELETEALL";
	private static final String DELETELOCAL = "DELETELOCAL";
	private static final String SPQUERYALL = "SPQUERYALL";
	private static final String EMPTY = "EMPTY";
	private static final String NOVERSION = "0";


	private static final String MSGTYPE = "MSGTYPE";
	private static final String KEYVAL = "KEYVAL";
	private static final String CONTENTVAL = "CONTENTVAL";
	private static final String RECVAVD = "RECVAVD";
	private static final String SENDAVD = "SENDAVD";
	private static final String VERSIONR = "VERSIONR";

	private static final String STATUS_INSERTED = "STATUS_INSERTED";
	private static final String STATUS_DELETED = "STATUS_DELETED";


	private static final int TIMEOUT = 5000;

	private static final int READERSIZE = 2;
	private static final int WRITERSIZE = 2;


	String[] returnQueryResultLocal = new String[2];
	String[] returnQueryResultRemote = new String[2];

	boolean queryStatus = false;
	boolean queryAllStatus = false;
	String queryAllStringResults = "";
	HashMap<String, String> keyVersionPair = new HashMap<String, String>();

	private Executor MYEXECUTOR = Executors.newFixedThreadPool(10);



	@Override
	public boolean onCreate() {


//		/** Test **/
//		try{
//
//			ArrayList<String> mylist = new ArrayList<String>();
////        try{
//            mylist.add(genHash("5554"));
//            mylist.add(genHash("5556"));
//            mylist.add(genHash("5558"));
//            mylist.add(genHash("5560"));
//            mylist.add(genHash("5562"));
//            mylist.add(genHash("4poOcNGjsqkt3B6scwjHG9sNN9FuHkHu"));
//            Collections.sort(mylist);
//
//			for(String m : mylist){
//				Log.e(TAG,m);
//			}
//
//			Log.e(TAG,getCoordinator("4poOcNGjsqkt3B6scwjHG9sNN9FuHkHu"));
////
////        }catch(Exception e ){
////            e.printStackTrace();
////        }
//
//
////			Log.e(TAG,genHash("5562"));
//			Log.e(TAG,genHash("4poOcNGjsqkt3B6scwjHG9sNN9FuHkHu"));
////			Log.e(TAG,genHash("5560"));
//
//
//
//		} catch (NoSuchAlgorithmException e){
//			e.printStackTrace();
//		}
//
//
//		/** Test **/

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		MYPORT = String.valueOf((Integer.parseInt(portStr) * 2));
		MYAVD = portStr;
		Log.i(TAG, "my AVD number is : " + MYAVD + " my port is : " + MYPORT);
		try {
//			/******/
//			ServerSocket serverSocket = new ServerSocket();
//			serverSocket.setReuseAddress(true);
//			serverSocket.bind(new InetSocketAddress(SERVER_PORT));
//			/******/
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.i("Create Server Socket", "Successful");

		} catch (Exception e) {
			Log.e("OnCreate : ", "Fail to create ServerSocket");
			e.printStackTrace();

		}
		// TODO : When a node fails and relaunch, it should ask for the latest version it shall mentain

		/**New added after phase 3**/

		Log.i(TAG,"QueryAll after failing");
//
		String myPreNode1 = getPreccessor(MYAVD);
		String myPreNode2 = getPreccessor(myPreNode1);
		String mySuccessor1 = getSuccessor(MYAVD);
		String mySuccessor2 = getSuccessor(mySuccessor1);
//
//		// TODO : For myPreNode1 : getCoordinate(key) == myPreNode1
//		// TODO : For myPreNode2 : getCoordinate(key) == myPreNode2
//		// TODO : For mySuccessor1 : getCoordinate(key) == MYAVD
//		// TODO : For mySuccessor2 : getCoordinate(key) == MYAVD
//
		JSONObject specialObjP1 = new JSONObject();
		JSONObject specialObjP2 = new JSONObject();
		JSONObject specialObjS1 = new JSONObject();
		JSONObject specialObjS2 = new JSONObject();

		String spReturnP1 = "";
		String spReturnP2 = "";
		String spReturnS1 = "";
		String spReturnS2 = "";
//
//		HashMap<String, String> keyValuePair = new HashMap<String, String>();
		try {
			specialObjP1.put(MSGTYPE, SPQUERYALL);
			specialObjP1.put(SENDAVD, MYAVD);
			specialObjP1.put(RECVAVD, myPreNode1);
			AsyncTask<String, Void, String> spRequestP1 = new ClientTask();
			spRequestP1.executeOnExecutor(MYEXECUTOR, specialObjP1.toString());
			spReturnP1 = spRequestP1.get();
//			Log.e(TAG,spReturnP1);

			specialObjP2.put(MSGTYPE, SPQUERYALL);
			specialObjP2.put(SENDAVD, MYAVD);
			specialObjP2.put(RECVAVD, myPreNode2);
			AsyncTask<String, Void, String> spRequestP2 = new ClientTask();
			spRequestP2.executeOnExecutor(MYEXECUTOR, specialObjP2.toString());
			spReturnP2 = spRequestP2.get();

			specialObjS1.put(MSGTYPE, SPQUERYALL);
			specialObjS1.put(SENDAVD, MYAVD);
			specialObjS1.put(RECVAVD, mySuccessor1);
			AsyncTask<String, Void, String> spRequestS1 = new ClientTask();
			spRequestS1.executeOnExecutor(MYEXECUTOR, specialObjS1.toString());
			spReturnS1 = spRequestS1.get();


			specialObjS2.put(MSGTYPE, SPQUERYALL);
			specialObjS2.put(SENDAVD, MYAVD);
			specialObjS2.put(RECVAVD, mySuccessor2);
			AsyncTask<String, Void, String> spRequestS2 = new ClientTask();
			spRequestS2.executeOnExecutor(MYEXECUTOR, specialObjS2.toString());
			spReturnS2 = spRequestS2.get();
		} catch (JSONException e){
			e.printStackTrace();
		} catch (ExecutionException e){
			e.printStackTrace();
		} catch (InterruptedException e){
			e.printStackTrace();
		}

		if(spReturnP1 != null && !spReturnP1.equals(EMPTY) && !spReturnP1.equals("")){
			String[] tokens = spReturnP1.split(",");
			ArrayList<String> temp = new ArrayList<String>();
			for (int j = 0; j < tokens.length; j++) {
				if (!tokens[j].isEmpty()) {
					temp.add(tokens[j]);
				}
			}

			for (int j = 0; j < temp.size(); j += 3) {
//				Log.i("Oncreate", "QueryAll return " + " key : " + temp.get(j) + " value : " + temp.get(j + 1) +
//						" version : " + temp.get(j + 2) + " myPreNode : " + myPreNode1 +
//						"Right Node : " + getCoordinator(temp.get(j)));
				String keyTemp = temp.get(j);
				String valTemp = temp.get(j + 1);
				String versionTemp = temp.get(j + 2);
				if (getCoordinator(keyTemp).equals(myPreNode1)) {
					String myVersion = getVersionOfKey(keyTemp);
					if (Integer.valueOf(versionTemp) >= Integer.valueOf(myVersion)) {
						insertLocalFunc(keyTemp, valTemp, versionTemp);
					}
				}
			}

		}

		if(spReturnP2 != null && !spReturnP2.equals(EMPTY) && !spReturnP2.equals("")){
			String[] tokens = spReturnP2.split(",");
			ArrayList<String> temp = new ArrayList<String>();
			for (int j = 0; j < tokens.length; j++) {
				if (!tokens[j].isEmpty()) {
					temp.add(tokens[j]);
				}
			}

			for (int j = 0; j < temp.size(); j += 3) {
//				Log.i("Oncreate", "QueryAll return " + " key : " + temp.get(j) + " value : " + temp.get(j + 1) +
//						" version : " + temp.get(j + 2) + " myPreNode : " + myPreNode1 +
//						"Right Node : " + getCoordinator(temp.get(j)));
				String keyTemp = temp.get(j);
				String valTemp = temp.get(j + 1);
				String versionTemp = temp.get(j + 2);
				if (getCoordinator(keyTemp).equals(myPreNode2)) {
					String myVersion = getVersionOfKey(keyTemp);
					if (Integer.valueOf(versionTemp) >= Integer.valueOf(myVersion)) {
						insertLocalFunc(keyTemp, valTemp, versionTemp);
					}
				}
			}

		}

		if(spReturnS1 != null && !spReturnS1.equals(EMPTY) && !spReturnS1.equals("")){
			String[] tokens = spReturnS1.split(",");
			ArrayList<String> temp = new ArrayList<String>();
			for (int j = 0; j < tokens.length; j++) {
				if (!tokens[j].isEmpty()) {
					temp.add(tokens[j]);
				}
			}

			for (int j = 0; j < temp.size(); j += 3) {
//				Log.i("Oncreate", "QueryAll return " + " key : " + temp.get(j) + " value : " + temp.get(j + 1) +
//						" version : " + temp.get(j + 2) + " myPreNode : " + myPreNode1 +
//						"Right Node : " + getCoordinator(temp.get(j)));
				String keyTemp = temp.get(j);
				String valTemp = temp.get(j + 1);
				String versionTemp = temp.get(j + 2);
				if (getCoordinator(keyTemp).equals(MYAVD)) {
					String myVersion = getVersionOfKey(keyTemp);
					if (Integer.valueOf(versionTemp) >= Integer.valueOf(myVersion)) {
						insertLocalFunc(keyTemp, valTemp, versionTemp);
					}
				}
			}

		}

		if(spReturnS2 != null && !spReturnS2.equals(EMPTY) && !spReturnS2.equals("")){
			String[] tokens = spReturnS2.split(",");
			ArrayList<String> temp = new ArrayList<String>();
			for (int j = 0; j < tokens.length; j++) {
				if (!tokens[j].isEmpty()) {
					temp.add(tokens[j]);
				}
			}

			for (int j = 0; j < temp.size(); j += 3) {
//				Log.i("Oncreate", "QueryAll return " + " key : " + temp.get(j) + " value : " + temp.get(j + 1) +
//						" version : " + temp.get(j + 2) + " myPreNode : " + myPreNode1 +
//						"Right Node : " + getCoordinator(temp.get(j)));
				String keyTemp = temp.get(j);
				String valTemp = temp.get(j + 1);
				String versionTemp = temp.get(j + 2);
				if (getCoordinator(keyTemp).equals(MYAVD)) {
					String myVersion = getVersionOfKey(keyTemp);
					if (Integer.valueOf(versionTemp) >= Integer.valueOf(myVersion)) {
						insertLocalFunc(keyTemp, valTemp, versionTemp);
					}
				}
			}

		}

		if(spReturnP1 != null && spReturnP2 != null && spReturnS1 != null && spReturnS2 != null){
			Log.e("Delete All", "from " + MYAVD);
			if(spReturnP1.equals(EMPTY) || spReturnP2.equals(EMPTY) ||
						spReturnS1.equals(EMPTY) || spReturnS2.equals(EMPTY)){
					deleteLocalAllFunc();
			}
		}

		/**New added after phase 3**/

		return false;
//		return true;
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		Log.i(TAG, "Trying to delete " + selection + " from " + MYAVD);

		if(selection.equals("@")){
			deleteLocalAllFunc();
		} else if(selection.equals("*")){
			deleteLocalAllFunc();
			for(int i = 0; i < DynamoRing.length; i++){
				if(MYAVD.equals(DynamoRing[i])){
					continue;
				} else {
					JSONObject sendOutObj = new JSONObject();
					try{
						sendOutObj.put(MSGTYPE,DELETEALL);
						sendOutObj.put(RECVAVD, DynamoRing[i]);
						sendOutObj.put(SENDAVD,MYAVD);
						AsyncTask<String, Void, String> sendDeleteAll = new ClientTask();
						sendDeleteAll.executeOnExecutor(MYEXECUTOR, sendOutObj.toString());
					} catch (JSONException e){
						e.printStackTrace();
						Log.e(TAG, "Failed to create DeleteAll-Type JsonObject");
					}
				}
			}
		} else {
			try{
				//String coordinator = "";
				String coordinator = getCoordinator(selection);
//				if(MYAVD.equals(coordinator)){
//					int writeSuccessCondition = 0;
//
//					deleteLocalFunc(selection);
//					writeSuccessCondition++;
//
//					Log.d(TAG, "Delete local key " + selection + "from AVD : " + MYAVD);
//
////					String successor1 = "";
////					String successor2 = "";
//					String successor1 = getSuccessor(MYAVD);
//					String successor2 = getSuccessor(getSuccessor(MYAVD));
//
//					if(successor1 == "" || successor2 ==""){
//						Log.e(TAG, "Failed to get successor for " + MYAVD);
//					}
//
//					JSONObject sendOutObject = new JSONObject();
//					sendOutObject.put(MSGTYPE,DELETE);
//					sendOutObject.put(RECVAVD,successor1);
//					sendOutObject.put(SENDAVD,MYAVD);
//					sendOutObject.put(KEYVAL,selection);
//
//
//					AsyncTask<String,Void,String> sendDeleteKey = new ClientTask();
//					sendDeleteKey.executeOnExecutor(MYEXECUTOR,sendOutObject.toString());
//					String ackResult = sendDeleteKey.get();
//					if(ackResult.equals(STATUS_DELETED)){
//						writeSuccessCondition++;
//					}
//
//					if(writeSuccessCondition >= WRITERSIZE){
//						Log.d(TAG, "Delete Successfully ( Key :  " + selection + " )");
//					}
//
//					sendOutObject.put(RECVAVD,successor2);
//					AsyncTask<String,Void,String> sendDeleteKey2 = new ClientTask();
//					sendDeleteKey2.executeOnExecutor(MYEXECUTOR,sendOutObject.toString());
//					ackResult = sendDeleteKey2.get();
//					if(ackResult.equals(STATUS_DELETED)){
//						writeSuccessCondition++;
//					}
//
//					if(writeSuccessCondition >= WRITERSIZE){
//						Log.d(TAG, "Delete Successfully ( Key :  " + selection + " )");
//					}
//
//				} else{

//					String successor1 = "";
//					String successor2 = "";
					String successor1 = getSuccessor(MYAVD);
					String successor2 = getSuccessor(successor1);

					JSONObject sendOutObject = new JSONObject();
					sendOutObject.put(MSGTYPE,DELETE);
					sendOutObject.put(RECVAVD,coordinator);
					sendOutObject.put(KEYVAL,selection);
					sendOutObject.put(SENDAVD,MYAVD);
					AsyncTask<String,Void,String> sendDeleteKey = new ClientTask();
					sendDeleteKey.executeOnExecutor(MYEXECUTOR,sendOutObject.toString());

					sendOutObject.put(RECVAVD,successor1);
					AsyncTask<String,Void,String> sendDeleteKey1 = new ClientTask();
					sendDeleteKey1.executeOnExecutor(MYEXECUTOR,sendOutObject.toString());

					sendOutObject.put(RECVAVD,successor2);
					AsyncTask<String,Void,String> sendDeleteKey2 = new ClientTask();
					sendDeleteKey2.executeOnExecutor(MYEXECUTOR,sendOutObject.toString());

//				}

			} catch (Exception e){
				e.printStackTrace();
				Log.e(TAG, "Failed to delete the key "+ selection);
			}
		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {

		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {

		String keyV = values.get("key").toString();
		String valueOfK = values.get("value").toString();

//		if(keyVersionPair.containsKey(keyV)){
//			int curV = Integer.valueOf(keyVersionPair.get(keyV));
//			keyVersionPair.put(keyV, String.valueOf(curV + 1));
//		} else{
//			keyVersionPair.put(keyV,String.valueOf(0));
//		}

		Log.i(TAG, "Trying to Insert : Key : " + keyV + " Values of K : " + valueOfK);
		try{

			String coordinator = getCoordinator(keyV);
			Log.i(keyV, "The coordinate is : " + coordinator + " My AVD Number is : " + MYAVD);


			int writeSuccessCondition = 0;

			String successor1 = getSuccessor(coordinator);
			String successor2 = getSuccessor(successor1);

			JSONObject insertObj = new JSONObject();
			insertObj.put(MSGTYPE,INSERT);
			insertObj.put(RECVAVD,coordinator);
			insertObj.put(SENDAVD,MYAVD);
			insertObj.put(KEYVAL,keyV);
			insertObj.put(CONTENTVAL,valueOfK);
			String ackresult;

			if(coordinator.equals(MYAVD)){
				String version = getVersionOfKey(keyV);
				String newVersion = String.valueOf(Integer.valueOf(version) + 1);
				insertLocalFunc(keyV,valueOfK,newVersion);

			} else {
				AsyncTask<String,Void,String> sendInsertToCoor = new ClientTask();
				sendInsertToCoor.executeOnExecutor(MYEXECUTOR,insertObj.toString());

				ackresult = sendInsertToCoor.get();
				if(STATUS_INSERTED.equals(ackresult)){
					writeSuccessCondition++;
				}

				if(ackresult != null){
					Log.i(ackresult,keyV);
				} else{
					Log.i("Failed rec ack", keyV);
				}
			}

//			AsyncTask<String,Void,String> sendInsertToCoor = new ClientTask();
//			sendInsertToCoor.executeOnExecutor(MYEXECUTOR,insertObj.toString());
//
//			String ackresult = sendInsertToCoor.get();
//			if(STATUS_INSERTED.equals(ackresult)){
//				writeSuccessCondition++;
//			}
//
//			if(ackresult != null){
//				Log.i(ackresult,keyV);
//			} else{
//				Log.i("Failed rec ack", keyV);
//			}

			insertObj.put(RECVAVD,successor1);

			if(successor1.equals(MYAVD)){
				String version = getVersionOfKey(keyV);
				String newVersion = String.valueOf(Integer.valueOf(version) + 1);
				insertLocalFunc(keyV,valueOfK,newVersion);

			} else {
				AsyncTask<String,Void,String> sendInsert1 = new ClientTask();
				sendInsert1.executeOnExecutor(MYEXECUTOR,insertObj.toString());

				ackresult = sendInsert1.get();
				if(STATUS_INSERTED.equals(ackresult)){
					writeSuccessCondition++;
				}

				if(ackresult != null){
					Log.i(ackresult,keyV);
				} else{
					Log.i("Failed rec ack", keyV);
				}
			}

//			AsyncTask<String,Void,String> sendInsert1 = new ClientTask();
//			sendInsert1.executeOnExecutor(MYEXECUTOR,insertObj.toString());
//
//			ackresult = sendInsert1.get();
//			if(STATUS_INSERTED.equals(ackresult)){
//				writeSuccessCondition++;
//			}
//
//			if(ackresult != null){
//				Log.i(ackresult,keyV);
//			} else{
//				Log.i("Failed rec ack", keyV);
//			}


			insertObj.put(RECVAVD,successor2);

			if(successor2.equals(MYAVD)){
				String version = getVersionOfKey(keyV);
				String newVersion = String.valueOf(Integer.valueOf(version) + 1);
				insertLocalFunc(keyV,valueOfK,newVersion);
			} else {

				AsyncTask<String,Void,String> sendInsert2 = new ClientTask();
				sendInsert2.executeOnExecutor(MYEXECUTOR,insertObj.toString());
				ackresult = sendInsert2.get();
				if(STATUS_INSERTED.equals(ackresult)){
					writeSuccessCondition++;
				}

				if(ackresult != null){
					Log.i(ackresult,keyV);
				} else{
					Log.i("Failed rec ack", keyV);
				}

				if(writeSuccessCondition >=2 ){
					Log.d(TAG, "Inserted Successfully ( Key :  " + keyV + " Values of K : " + valueOfK + " )");
					return uri;
				}

			}
//
//			AsyncTask<String,Void,String> sendInsert2 = new ClientTask();
//			sendInsert2.executeOnExecutor(MYEXECUTOR,insertObj.toString());
//			ackresult = sendInsert2.get();
//			if(STATUS_INSERTED.equals(ackresult)){
//				writeSuccessCondition++;
//			}
//
//			if(ackresult != null){
//				Log.i(ackresult,keyV);
//			} else{
//				Log.i("Failed rec ack", keyV);
//			}
//
//			if(writeSuccessCondition >=2 ){
//				Log.d(TAG, "Inserted Successfully ( Key :  " + keyV + " Values of K : " + valueOfK + " )");
//				return uri;
//			}

		} catch (Exception e){
			e.printStackTrace();
			Log.e(TAG, "Failed to insert the key "+ keyV);
		}
		return null;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		if(selection.equals("@")){
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
			String[] filenameList = getContext().fileList();
			String [] accessTable = {"key","value"};
			MatrixCursor cursor = new MatrixCursor(accessTable);


			JSONObject queryAllObject = new JSONObject();
			ArrayList<String> temp = new ArrayList<String>();
			try{
				queryAllObject.put(MSGTYPE, QUERYALL);
				queryAllObject.put(SENDAVD, MYAVD);
				for(int i = 0; i < DynamoRing.length; i++){
					queryAllObject.put(RECVAVD,DynamoRing[i]);
					AsyncTask<String, Void, String> queryAllTask = new ClientTask();
					queryAllTask.executeOnExecutor(MYEXECUTOR,queryAllObject.toString());
					String returnResult = queryAllTask.get();
					// TODO : Save the return String, Parse them and return the cursor - Done
					if(returnResult != null){
						Log.i(TAG,"Query All request return " + returnResult + " From " + DynamoRing[i]);

						String[] tokens = returnResult.split(",");

						for(int j = 0; j < tokens.length; j++){
							if(!tokens[j].isEmpty()){
								temp.add(tokens[j]);
							}
						}

						for(int j= 0; j < temp.size(); j +=2 ){
							Log.i(TAG,"QueryAll return " + " key : " + temp.get(j) + " value : " + temp.get(j+1));
							cursor.addRow(new String[]{temp.get(j),temp.get(j+1)});
						}
					} else {
						continue;
					}

				}

				return cursor;
			} catch (JSONException e){
				e.printStackTrace();
			} catch (InterruptedException e){
				e.printStackTrace();
			} catch (ExecutionException e){
				e.printStackTrace();
			}
		} else {
			String coordinator = getCoordinator(selection);
			JSONObject queryCoordinate = new JSONObject();
			JSONObject querySuccessor1 = new JSONObject();
			JSONObject querySuccessor2 = new JSONObject();

			String successor1 = getSuccessor(coordinator);
			String successor2 = getSuccessor(successor1);

			try{
				queryCoordinate.put(MSGTYPE,QUERY);
				queryCoordinate.put(SENDAVD,MYAVD);
				queryCoordinate.put(RECVAVD,coordinator);
				queryCoordinate.put(KEYVAL,selection);

				querySuccessor1.put(MSGTYPE, QUERY);
				querySuccessor1.put(SENDAVD, MYAVD);
				querySuccessor1.put(RECVAVD, successor1);
				querySuccessor1.put(KEYVAL, selection);

				querySuccessor2.put(MSGTYPE,QUERY);
				querySuccessor2.put(SENDAVD,MYAVD);
				querySuccessor2.put(RECVAVD,successor2);
				querySuccessor2.put(KEYVAL, selection);

			} catch (JSONException e){
				e.printStackTrace();
			}

			try{
				AsyncTask<String, Void, String> querySelectionTask = new ClientTask();
				querySelectionTask.executeOnExecutor(MYEXECUTOR,queryCoordinate.toString());
				String resFromCoor = querySelectionTask.get();

				if(resFromCoor != null){
					Log.i("Query Result Return",resFromCoor);
				}

//				JSONObject resFromCoorObj0 = new JSONObject(resFromCoor);

				AsyncTask<String, Void, String> querySelectionTask1 = new ClientTask();
				querySelectionTask1.executeOnExecutor(MYEXECUTOR,querySuccessor1.toString());
				String resFromSuc1 = querySelectionTask1.get();

				if(resFromSuc1 != null){
					Log.i("Query Result Return",resFromSuc1);
				}

//				JSONObject resFromSucObj1 = new JSONObject(resFromSuc1);

				AsyncTask<String,Void,String> querySelectionTask2 = new ClientTask();
				querySelectionTask2.executeOnExecutor(MYEXECUTOR,querySuccessor2.toString());
				String resFromSuc2 = querySelectionTask2.get();

				if(resFromSuc2 != null){
					Log.i("Query Result Return",resFromSuc2);
				}

//				JSONObject resFromSucObj2 = new JSONObject(resFromSuc2);

//				String version0 = resFromCoorObj0.get(VERSIONR).toString();
//				String version1 = resFromSucObj1.get(VERSIONR).toString();
//				String version2 = resFromSucObj2.get(VERSIONR).toString();

				String [] accessTable = {"key","value"};
				MatrixCursor cursor = new MatrixCursor(accessTable);

				if(resFromSuc1 != null && resFromCoor != null && resFromSuc2 != null){
					JSONObject resFromCoorObj0 = new JSONObject(resFromCoor);
					JSONObject resFromSucObj1 = new JSONObject(resFromSuc1);
					JSONObject resFromSucObj2 = new JSONObject(resFromSuc2);

					String version0 = resFromCoorObj0.get(VERSIONR).toString();
					String version1 = resFromSucObj1.get(VERSIONR).toString();
					String version2 = resFromSucObj2.get(VERSIONR).toString();


					if((Integer.valueOf(version0) >= Integer.valueOf(version1)) &&
							(Integer.valueOf(version0) >= Integer.valueOf(version2))){
						String[] newRowLocal = {resFromCoorObj0.get(KEYVAL).toString(),resFromCoorObj0.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);

					} else if ((Integer.valueOf(version1) >= Integer.valueOf(version0)) &&
							(Integer.valueOf(version1) >= Integer.valueOf(version2))){
						String[] newRowLocal = {resFromSucObj1.get(KEYVAL).toString(),resFromSucObj1.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);

					} else if((Integer.valueOf(version2) >= Integer.valueOf(version0)) &&
							(Integer.valueOf(version2) >= Integer.valueOf(version1))){
						String[] newRowLocal = {resFromSucObj2.get(KEYVAL).toString(),resFromSucObj2.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);

					}

//					Log.i("Add to cursor", "Key : " + KEYVAL + "Content : " + CONTENTVAL + " condition 1");

				} else if(resFromCoor != null && resFromSuc1 != null && resFromSuc2 == null){
					JSONObject resFromCoorObj0 = new JSONObject(resFromCoor);
					JSONObject resFromSucObj1 = new JSONObject(resFromSuc1);
					String version0 = resFromCoorObj0.get(VERSIONR).toString();
					String version1 = resFromSucObj1.get(VERSIONR).toString();

					if((Integer.valueOf(version0) >= Integer.valueOf(version1))){
						String[] newRowLocal = {resFromCoorObj0.get(KEYVAL).toString(),resFromCoorObj0.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					} else {
						String[] newRowLocal = {resFromSucObj1.get(KEYVAL).toString(),resFromSucObj1.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					}

//					Log.i("Add to cursor", "Key : " + KEYVAL + "Content : " + CONTENTVAL + " condition 2");


				} else if(resFromCoor != null && resFromSuc1 == null && resFromSuc2 != null){
					JSONObject resFromCoorObj0 = new JSONObject(resFromCoor);
					JSONObject resFromSucObj2 = new JSONObject(resFromSuc2);
					String version0 = resFromCoorObj0.get(VERSIONR).toString();
					String version2 = resFromSucObj2.get(VERSIONR).toString();

					if((Integer.valueOf(version0) >= Integer.valueOf(version2))){
						String[] newRowLocal = {resFromCoorObj0.get(KEYVAL).toString(),resFromCoorObj0.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					} else {
						String[] newRowLocal = {resFromSucObj2.get(KEYVAL).toString(),resFromSucObj2.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					}

//					Log.i("Add to cursor", "Key : " + KEYVAL + "Content : " + CONTENTVAL + " condition 3");


				} else if(resFromCoor == null && resFromSuc1 != null && resFromSuc2 != null) {
					JSONObject resFromSucObj1 = new JSONObject(resFromSuc1);
					JSONObject resFromSucObj2 = new JSONObject(resFromSuc2);
					String version1 = resFromSucObj1.get(VERSIONR).toString();
					String version2 = resFromSucObj2.get(VERSIONR).toString();

					if ((Integer.valueOf(version1) >= Integer.valueOf(version2))) {
						String[] newRowLocal = {resFromSucObj1.get(KEYVAL).toString(), resFromSucObj1.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					} else {
						String[] newRowLocal = {resFromSucObj2.get(KEYVAL).toString(), resFromSucObj2.get(CONTENTVAL).toString()};
						cursor.addRow(newRowLocal);
					}

//					Log.i("Add to cursor", "Key : " + KEYVAL + "Content : " + CONTENTVAL + " condition 4");
				}
				return cursor;
			} catch (InterruptedException e){
				e.printStackTrace();
			} catch (ExecutionException e){
				e.printStackTrace();
			} catch (JSONException e){
				e.printStackTrace();
			}

		}

		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {

		return 0;
	}


	/** Return the avd number of the coordinate**/
	public String getCoordinator(String key){
		String returnAVD = "";
		try{
			String hashKey = genHash(key);
			if(hashKey.compareTo(genHash("5562")) < 0 || hashKey.compareTo(genHash("5560")) > 0){
//				Log.e(TAG, "True");
//				Log.e(TAG, String.valueOf(hashKey.compareTo("5562")));
//				Log.e(TAG, String.valueOf(hashKey.compareTo("5560")));
				returnAVD = "5562";
//				return returnAVD;
			} else {
				for(int i = 0; i < DynamoRing.length; i++){

					if(hashKey.compareTo(genHash(DynamoRing[i])) > 0 && hashKey.compareTo(genHash(DynamoRing[i+1])) < 0 ){
//						Log.e(TAG, i + DynamoRing[i]);
						returnAVD =  DynamoRing[i+1];
						break;
					} else {
						continue;
					}
				}
			}
		} catch (Exception e){
			e.printStackTrace();
			Log.e(TAG, "Can't get Coordinator");
		}
		return returnAVD;
	}

	public String getSuccessor(String avdNumber){
		String returnAVD = "";
		if (avdNumber.equals("5560")){
			returnAVD = "5562";
			return returnAVD;
		} else {
			for(int i = 0; i < DynamoRing.length; i++){
				if(avdNumber.equals(DynamoRing[i])){
					returnAVD = DynamoRing[i+1];
					return returnAVD;
				} else {
					continue;
				}
			}
		}
		return returnAVD;
	}


	public String getPreccessor(String avdNumber){
		String returnAVD = "";
		if (avdNumber.equals("5562")){
			returnAVD = "5560";
			return returnAVD;
		} else {
			for(int i = 1; i < DynamoRing.length; i++){
				if(avdNumber.equals(DynamoRing[i])){
					returnAVD = DynamoRing[i -1];
					return returnAVD;
				} else {
					continue;
				}
			}
		}
		return returnAVD;
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

	public void insertLocalFunc(String key, String value, String version){
		try{
			//String keyId = genHash(key);
			String keyId = key;
			FileOutputStream fos = getContext().openFileOutput(keyId,Context.MODE_PRIVATE);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			bw.write(value);
			bw.newLine();
			bw.write(version);
			bw.flush();
			bw.close();
		} catch (Exception e){
			Log.e("Insert Local File List","Fail to write to the file");
		}

	}

	public String[] queryLocalFunc(String key){

		try{
			String[] queryResultReturn = new String[2];
//			Log.i("Query Local Func", key);
			//String filename = genHash(key);
			String filename = key;
			StringBuffer content = new StringBuffer();

			FileInputStream fIn = getContext().openFileInput(filename);
			InputStreamReader isr = new InputStreamReader(fIn);
			BufferedReader bufferedReader = new BufferedReader(isr);

			String readString = bufferedReader.readLine();
//			Log.e(TAG,readString);
			content.append(readString);
//			while (readString != null){
//				content.append(readString);
////				readString = bufferedReader.readLine();
//			}
			bufferedReader.close();
			isr.close();

//			Log.i("Query Local Func",key);
//			Log.i("Query Local Func",content.toString());

			String contentVal = content.toString();
			queryResultReturn[0] = key;
			queryResultReturn[1] = contentVal;
//			Log.e("Return Query Local Func",queryResultReturn[0]);
//			Log.e("Return Query Local Func",queryResultReturn[1]);
			return queryResultReturn;


		} catch (Exception e){
			Log.i("Query Local Func","query failed");
			e.printStackTrace();

		}
		Log.v("query", key);
		return null;
	}

	public void deleteLocalFunc(String key){
		try{
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

	public class ServerTask extends AsyncTask<ServerSocket,String,Void>{

		@Override
		protected Void doInBackground(ServerSocket... sockets){
			ServerSocket serverSocket = sockets[0];
			while (true){
				try{

					Socket s = serverSocket.accept();
					Log.i("ServerTask","received Packet");
	//					DataInputStream dis = new DataInputStream(s.getInputStream());
	//					String message = dis.readUTF();
					BufferedReader inputS = new BufferedReader(new InputStreamReader(s.getInputStream()));
					String message = inputS.readLine();
					Log.i("Server Task","received Message" + message);
					JSONObject receivedMsg = new JSONObject(message);
					String messageType = receivedMsg.get(MSGTYPE).toString();
					if(messageType.equals(DELETE)){
	//						 TODO : Delete local key-value pair ---- Done
	//						 TODO : Delete AVD that belongs to this AVD ---Done
						String deleteKey = receivedMsg.get(KEYVAL).toString();
						deleteLocalFunc(deleteKey);
						PrintWriter out = new PrintWriter(s.getOutputStream(),true); //Reference : https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html
						out.println(STATUS_DELETED);

					} else if(messageType.equals(DELETEALL)){

						deleteLocalAllFunc();

					} else if(messageType.equals(INSERT)){

						Log.i(TAG,"InsertReceived ");
						String key = receivedMsg.get(KEYVAL).toString();
						Log.i(TAG,"InsertReceived " + "Key : " + key);

						String preVersion = getVersionOfKey(key);
						int verTemp = Integer.valueOf(preVersion) + 1;
						String newVersion = String.valueOf(verTemp);
						String value = receivedMsg.get(CONTENTVAL).toString();
						insertLocalFunc(key,value, newVersion);
						Log.i("Server","Insert Key : " + key + " Value : " + value + " Version : " + newVersion);
						PrintWriter out = new PrintWriter(s.getOutputStream(),true);
						out.println(STATUS_INSERTED);

					} else if(messageType.equals(QUERY)){
	//						Log.i()
						String key = receivedMsg.get(KEYVAL).toString();
						String[] temp = queryLocalFunc(key);
	//						String version = keyVersionPair.get(key);
						String version = getVersionOfKey(key);
						//String returnRes = temp[0] + "," + temp[1] + "," + version;
						JSONObject returnJsonObj = new JSONObject();
						returnJsonObj.put(KEYVAL, temp[0]);
						returnJsonObj.put(CONTENTVAL,temp[1]);
						returnJsonObj.put(VERSIONR,version);

						PrintWriter out = new PrintWriter(s.getOutputStream(),true);
						out.println(returnJsonObj.toString());

					} else if(messageType.equals(QUERYALL)){
						String[] filenameList = getContext().fileList();
						StringBuilder str = new StringBuilder();

						Log.i("Client Side Query ALL", "Try to query local all");

						for(int i = 0; i< filenameList.length; i++){
							String filename = filenameList[i];
							returnQueryResultLocal = queryLocalFunc(filename);
							//String[] newRowLocal = {returnQueryResultLocal[0],returnQueryResultLocal[1]};
							str.append(returnQueryResultLocal[0]);
							str.append(",");
							str.append(returnQueryResultLocal[1]);
							str.append(",");
						}
						String tempRes = str.toString();
						Log.i("From Client Side", tempRes);
						PrintWriter out = new PrintWriter(s.getOutputStream(),true);
						out.println(tempRes);
					} else if(messageType.equals(SPQUERYALL)){
						/** New Added For Phase 3  **/
						Log.i(TAG,"Server received " + SPQUERYALL + " type Message");
						String[] filenameList = getContext().fileList();

						if(isEmptyStringArray(filenameList)){
							PrintWriter out = new PrintWriter(s.getOutputStream(),true);
							out.println(EMPTY);
						} else {
							StringBuilder str = new StringBuilder();
							int curLength = filenameList.length;

							Log.i("Client Side Query ALL", "Try to query local all");

							for(int i = 0; i< curLength; i++){
								String filename = filenameList[i];
								String version = getVersionOfKey(filename);
								returnQueryResultLocal = queryLocalFunc(filename);

								Log.i("Query Result", "Key : " + returnQueryResultLocal[0] +
										" Content : " + returnQueryResultLocal[1]);
								str.append(returnQueryResultLocal[0]);
								str.append(",");
								str.append(returnQueryResultLocal[1]);
								str.append(",");
								str.append(version);
								str.append(",");
							}
							String tempRes = str.toString();
							Log.i("Length" , String.valueOf(tempRes.length()));
							PrintWriter out = new PrintWriter(s.getOutputStream(),true);
							out.println(tempRes);
						}

						/** New Added For Phase 3  **/
					}
				} catch (Exception e){
					Log.e("Server", "Problem while recieving: "+e.getMessage());
					e.printStackTrace();

				}

			}
		}

		protected void onProgressUpdate(String... msgs){
			if(msgs.length == 1) {
				String m = msgs[0];
				Log.e(TAG,m);
				new ClientTask().executeOnExecutor(MYEXECUTOR, m);
			}
		}
	}

	public class ClientTask extends AsyncTask<String,Void, String> {
		@Override
		protected String doInBackground(String... msgs){
			try{
				String receivedMsg = msgs[0];

				Log.i(TAG, "Client received Message " + msgs[0]);

				JSONObject receivedObject = new JSONObject(receivedMsg);
				String messageType = receivedObject.get(MSGTYPE).toString();

				if(messageType.equals(INSERT)){

					Log.i(TAG, "Received Message type is " + messageType);
					String recvAVD = receivedObject.get(RECVAVD).toString();
					Log.i(TAG, "Received Message receive AVD is " + recvAVD);
					String remotePort = String.valueOf((Integer.parseInt(recvAVD))*2);
					Log.i(TAG, "Received Message remote Port is " + remotePort);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					Log.i("Client Task","Socket Created Successfully");
					socket.setSoTimeout(TIMEOUT);
					PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
					out.println(receivedMsg);
					Log.i("Insert : " , receivedMsg);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String ackMsg = in.readLine();
					Log.i("Client : ", "get the ack msg from Server " + ackMsg);
					socket.close();
					out.close();
					in.close();
					return ackMsg;

				} else if(messageType.equals(QUERY) || messageType.equals(QUERYALL)){
					String recvAVD = receivedObject.get(RECVAVD).toString();
					String remotePort = String.valueOf((Integer.parseInt(recvAVD))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					socket.setSoTimeout(TIMEOUT);
					PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
					out.println(receivedMsg);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String ackMsg = in.readLine();
					socket.close();
//					out.flush();
					out.close();
					in.close();
					return ackMsg;

				} else if(messageType.equals(DELETE) || messageType.equals(DELETEALL)){//Send the Message to the coordinate
					String recvAVD = receivedObject.get(RECVAVD).toString();
					String remotePort = String.valueOf((Integer.parseInt(recvAVD))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					socket.setSoTimeout(TIMEOUT);
					PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
					out.println(receivedMsg);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String ackMsg = in.readLine();
					socket.close();
					out.close();
//					out.flush();
					in.close();

					return ackMsg;
				} else if(messageType.equals(SPQUERYALL)){
					/** New Added For Phase 3  **/
					Log.i(TAG,"Client received " + SPQUERYALL + " request!");
					String recvAVD = receivedObject.get(RECVAVD).toString();
					String remotePort = String.valueOf((Integer.parseInt(recvAVD))*2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
							Integer.parseInt(remotePort));
					socket.setSoTimeout(TIMEOUT);
					PrintWriter out = new PrintWriter(socket.getOutputStream(),true);
					out.println(receivedMsg);
					BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					String ackMsg = in.readLine();
					Log.i(TAG,"Client try to get ackMsg");
					socket.close();
					out.close();
					in.close();

					return ackMsg;
					/** New Added For Phase 3 **/
				}
			} catch (Exception e){
				Log.e("ClientTask","Packet sent failed");
				e.printStackTrace();
			}
			return null;
		}
	}

	public boolean isEmptyStringArray(String [] array){
		for(int i=0; i<array.length; i++){
			if(array[i]!=null){
				return false;
			}
		}
		return true;
	}

	public String getVersionOfKey(String key){

		try{
			String filename = key;
			StringBuffer content = new StringBuffer();

			FileInputStream fIn = getContext().openFileInput(filename);
			InputStreamReader isr = new InputStreamReader(fIn);
			BufferedReader bufferedReader = new BufferedReader(isr);

			String readString = bufferedReader.readLine();
			String nextline = bufferedReader.readLine();
			content.append(nextline);
			isr.close();

			String version = content.toString();
			Log.i("GetVersionReturn : ",version);

			return version;

		} catch (FileNotFoundException e){
			Log.i("Query Local Func","query failed");
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}

		Log.i("GetVersionReturn : ","-1");
		return String.valueOf(-1);
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
