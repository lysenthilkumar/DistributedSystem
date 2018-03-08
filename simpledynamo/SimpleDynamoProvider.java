package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.AbstractThreadedSyncAdapter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;

//Self added
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;
import android.content.Context;
import android.util.Log;
import android.telephony.TelephonyManager;
import android.os.AsyncTask;
import java.io.IOException;
import java.util.ArrayList;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutionException;

import android.database.MatrixCursor;

public class SimpleDynamoProvider extends ContentProvider {


	//Global variables
	static final int SERVER_PORT = 10000;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";
	MatrixCursor matrixCursorGlobal = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
	MatrixCursor matrixCursorKey = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
	Boolean globalDumpStatus = true;
	Boolean onCreateStatus = false;

	//Arraylist to hold members of the chord
	ArrayList<String> hashedMemberList = new ArrayList<String>();
	ArrayList<Integer> memberList = new ArrayList<Integer>();

	//Class to manipulate the ArrayList of members of the chord
	class NodePosition {

		//Method to populate the members of the chord - hashed values of the port
		public void populateHashedList() {
			hashedMemberList.add(callHashFunction("5562"));
			hashedMemberList.add(callHashFunction("5556"));
			hashedMemberList.add(callHashFunction("5554"));
			hashedMemberList.add(callHashFunction("5558"));
			hashedMemberList.add(callHashFunction("5560"));
		}

		//Method to populate the members of the chord - port number
		public void populateList() {
			memberList.add(5562);
			memberList.add(5556);
			memberList.add(5554);
			memberList.add(5558);
			memberList.add(5560);
		}
	}


	//Variables to identify this AVD
	class NodeDetails {

		int index;
		int selfPortNumber = -1;
		String hashedSelfPortNumber = "NOTSET";

		int predecessor = -1;
		String hashedPredecessor = "NOTSET";
		int secondPredecessor = -1;
		String hashedSecondPredecessor = "NOTSET";

		int successor = -1;
		String hashedSuccessor = "NOTSET";
		int secondSuccessor = -1;
		String hashedSecondSuccessor = "NOTSET";


		public void setPredecessor() {

			if(index > 1) {
				predecessor = memberList.get(index-1);
				hashedPredecessor = callHashFunction(String.valueOf(predecessor));
				secondPredecessor = memberList.get(index-2);
				hashedSecondPredecessor = callHashFunction(String.valueOf(secondPredecessor));
			}
			else {

				if(index == 0) {
					predecessor = memberList.get(4);
					secondPredecessor = memberList.get(3);
				}
				else {
					predecessor = memberList.get(0);
					secondPredecessor = memberList.get(4);
				}

				hashedPredecessor = callHashFunction(String.valueOf(predecessor));
				hashedSecondPredecessor = callHashFunction(String.valueOf(secondPredecessor));
			}
		}

		public void setSuccessor() {

			if(index < 3) {
				successor = memberList.get(index+1);
				hashedSuccessor = callHashFunction(String.valueOf(successor));
				secondSuccessor = memberList.get(index+2);
				hashedSecondSuccessor = callHashFunction(String.valueOf(secondSuccessor));
			}
			else {

				if(index == 3) {
					successor = memberList.get(4);
					secondSuccessor = memberList.get(0);
				}
				else {
					successor = memberList.get(0);
					secondSuccessor = memberList.get(1);
				}

				hashedSuccessor = callHashFunction(String.valueOf(successor));
				hashedSecondSuccessor = callHashFunction(String.valueOf(secondSuccessor));
			}

		}

		public void NodeDetails(int thisAVDPortNumber) {

			selfPortNumber = thisAVDPortNumber;
			hashedSelfPortNumber = callHashFunction(String.valueOf(selfPortNumber));
			index = memberList.indexOf(selfPortNumber);

			setPredecessor();
			setSuccessor();

		}

	}

	NodeDetails node = new NodeDetails();

	//Method to find the position of the key in the chord
	public int findPartion(String hashedMessageKey) {

		for(int index = 1; index < 5; index++) {
			if (hashedMemberList.get(index - 1).compareTo(hashedMessageKey) < 0
					&&
					hashedMessageKey.compareTo(hashedMemberList.get(index)) <= 0) {
				return memberList.get(index);
			}
		}
		return memberList.get(0);
	}

	//Method to call the genHash function
	public String callHashFunction(String nodeID) {
		try {
			String hashedNodeID = genHash(nodeID);
			return hashedNodeID;
		} catch (NoSuchAlgorithmException e) {
			Log.d("Hash function call", "Just hashed: " + nodeID);
		}
		return null;
	}

	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}


	public final class FeedReaderContract {

		private FeedReaderContract() {
		}

		/* Inner class that defines the table contents */
		public class FeedEntry implements BaseColumns {
			public static final String TABLE_NAME = "keyValueTable";
			public static final String COLUMN_NAME_TITLE = "key";
			public static final String COLUMN_NAME_SUBTITLE = "value";
		}
	}

	private static final String SQL_CREATE_ENTRIES =
			"CREATE TABLE " + FeedReaderContract.FeedEntry.TABLE_NAME + " (" +
					FeedReaderContract.FeedEntry.COLUMN_NAME_TITLE + " TEXT PRIMARY KEY," +
					FeedReaderContract.FeedEntry.COLUMN_NAME_SUBTITLE + " TEXT" + "PRIMARY KEY)";

	private static final String SQL_DELETE_ENTRIES =
			"DROP TABLE IF EXISTS " + FeedReaderContract.FeedEntry.TABLE_NAME;

	public class FeedReaderDbHelper extends SQLiteOpenHelper {
		// If you change the database schema, you must increment the database version.
		public static final int DATABASE_VERSION = 1;
		public static final String DATABASE_NAME = "FeedReader.db";

		public FeedReaderDbHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
		}

		public void onCreate(SQLiteDatabase db) {
			db.execSQL(SQL_CREATE_ENTRIES);
		}

		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// This database is only a cache for online data, so its upgrade policy is
			// to simply to discard the data and start over
			db.execSQL(SQL_DELETE_ENTRIES);
			onCreate(db);
		}

		public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onUpgrade(db, oldVersion, newVersion);
		}
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub


		int rowsCount;
		FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = mDbHelper.getWritableDatabase();

		//if(selection.equals("*") || selection.equals("@")){
		Log.d("Delete", "Testing Delete");
		rowsCount = db.delete("keyValueTable", null, null);
		Log.d("Delete", "rowCount after * or @: " + rowsCount);
		return rowsCount;

		/*
		String messageToClientForDeletion = -1 + "$" + node.selfPortNumber;
		AsyncTask asyncClientTask = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientForDeletion);
		try {
			asyncClientTask.get();
			return rowsCount;
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		*/
		//}

		//rowsCount = db.delete("keyValueTable", "key=?", new String[] {selection});
		//Log.d("Delete", "First rowCount after key: " + selection +" is " + rowsCount);

		//return rowsCount;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		while(true) {
			if (onCreateStatus) {
				Log.d("Insert", "Inside insert()");
				//Split Content values object to find the key to check for if condition
				String newMessageKey = String.valueOf(values.get(KEY_FIELD));
				String hashedNewMessageKey = callHashFunction(newMessageKey);
				String newMessageValue = String.valueOf(values.get(VALUE_FIELD));

				Log.d("Insert", "Message being pushed is KEY: " + newMessageKey + " VALUE: " + newMessageValue);

				int partitionPortNumber = findPartion(hashedNewMessageKey);

				String messageToClient = 1 + "$" + partitionPortNumber + "$" + newMessageKey + "$" + newMessageValue;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClient);
			/*
			AsyncTask asyncClientTask = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClient);
			try {
				asyncClientTask.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
			*/
				return null;
			}
		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		//Populates the ArrayList of all members of the chord
		onCreateStatus = false;

		new NodePosition().populateList();
		new NodePosition().populateHashedList();

		//TelephonyManager to set port numbers to different avd's
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		//node.selfPortNumber = (Integer.parseInt(portStr)); //Find the port number of the AVD will be port number * 2

		node.NodeDetails(Integer.parseInt(portStr)); //Calling constructor

		Log.d("OnCreate", "Order of the chord is: " + node.secondPredecessor + "->" + node.predecessor + "->" + node.selfPortNumber + "->" + node.successor + "->" + node.secondSuccessor);
		Log.d("OnCreate", "Second Predecessor: " + node.secondPredecessor);
		Log.d("OnCreate", "Predecessor: " + node.predecessor);
		Log.d("OnCreate", "Node Port Number: " + node.selfPortNumber);
		Log.d("OnCreate", "Successor: " + node.successor);
		Log.d("OnCreate", "Second Successor: " + node.secondSuccessor);


		Log.d("OnCreate: ", "This AVD's Port number is: " + node.selfPortNumber);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			Log.d("OnCreate", "This AVD's Server is launching");
		} catch (IOException e) {
			Log.e("OnCreate", "Can't create a ServerSocket");
		}

		//Ping your node.predecessor and node.successor for their key and values db so that you can recover, Also handle null if their DB's are empty
		String messageToClientWithNeighbors = 0 + "$" + node.selfPortNumber;
		AsyncTask asyncTask = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientWithNeighbors);

		try {
			asyncTask.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

		onCreateStatus = true;
		Log.d("Completed", "onCreate is complete!");
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		while(true) {

			if (onCreateStatus) {

				Log.d("QueryBlock", "Inside query block!");
				FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
				SQLiteDatabase db = mDbHelper.getReadableDatabase();
				String[] selectionArray = new String[1];
				selectionArray[0] = selection;

				if (selection.equals("@")) {

					//This is for global dump meaning it is a local dump of all the nodes, Define a matrix cursor for global dump and update the content from all other nodes
					Log.d("QueryBlock", "Inside selection == @");
					Cursor localDump = db.query(
							FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
							null,                                  // The columns to return
							null,                                     // The columns for the WHERE clause
							null,                                     // The values for the WHERE clause
							null,                                     // don't group the rows
							null,                                     // don't filter by row groups
							null                                      // The sort order
					);
					return localDump;
				}

				else if (selection.equals("*")) {

					//This is for local dump for some node, return DB of the node, request your own client to ask server to collect all local dumos and build the matrix cursor
					matrixCursorGlobal = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
					Log.d("QueryBlock", "Inside selection == *");
					String messageToClientForGlobalDump = 3 + "$" + node.selfPortNumber;
					//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientForGlobalDump);
					AsyncTask asyncClientTask = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientForGlobalDump);

					try {
						asyncClientTask.get();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

					while (globalDumpStatus) {
						if (matrixCursorGlobal.getCount() > 74) {
							//globalDumpStatus = false;
							return matrixCursorGlobal;
						}

					}

				}

				else { //Run query for key search

					Log.d("TestModule", "query: Inside selection = " + selection);
					/*
					Cursor cursorObject = db.query(
							FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
							null,                               // The columns to return
							"key=?",                                  // The columns for the WHERE clause
							selectionArray,                            // The values for the WHERE clause
							null,                                     // don't group the rows
							null,                                     // don't filter by row groups
							null                                 // The sort order
					);
					*/
					Log.d("TestModule", "query: Cursor object called for: " + selection);

					//if(cursorObject.getCount() == 0) {

					//matrixCursorKey = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
					Log.d("TestModule", "query: Inside if(cursorObject.getCount() == 0) for: " + selection);
					String messageToClientForKey = 4 + "$" + node.selfPortNumber + "$" + selection;
					//new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientForKey);
					AsyncTask asyncClientTask = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientForKey);
					//matrixCursorKey = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
					try {

						asyncClientTask.get();  //Stops all other execution till client fetches the required key.
						return matrixCursorKey;
						//TODO: By the time this AsyncTask finishes execution the second lock takes the liberty to change the data in the matrixCursorkey

					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}

					//while (keySearchStatus) {
					//if(matrixCursorKey.getCount() > 0) {
					//Log.d("TestModule", "query: Inside while(keySearchStatus) for: " + selection);
					//keySearchStatus = false;
					//Log.d("TestModule", "query: Size of the matrixCursorKey: " + matrixCursorKey.getCount() +" for " + selection);
					//matrixCursorKey.moveToFirst();
					//String key = matrixCursorKey.getString(matrixCursorKey.getColumnIndex(KEY_FIELD));
					//String value = matrixCursorKey.getString(matrixCursorKey.getColumnIndex(VALUE_FIELD));
					//Log.d("TestModule", "query: matrixCursorKey.KEY_FIELD: " + key + "   matrixCursorValue.VALUE_FIELD: " + value);
					//Log.d("TestModule", "query: Served request for key: " + key);
					//return matrixCursorKey;
					//}
					//}

					//}

					//return cursorObject;
				}
				Log.d("Notification", "Please note NULL returning at query function! --------------------------------------------------------------------------------------\n\n\n\n\n");
				return null;
			}
		}

		//Log.d("Note: ", "Query() is returning OUTER null ----------------------------------------Note: Query() - OUTER NULL--------------------------------------------------------------------");
		//return null;

	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
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

	public void insertIntoSelf (String message) {
		while(true) {
			if (onCreateStatus) {
				String[] messageArray = message.split("\\$");
				String unhashedKey = messageArray[1];
				Log.d("insertIntoSelf", "Insertion code running for key: " + unhashedKey);
				String values = messageArray[2];
				ContentValues cvObjectToInsert = new ContentValues();
				cvObjectToInsert.put(KEY_FIELD, unhashedKey);
				cvObjectToInsert.put(VALUE_FIELD, values);
				FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
				SQLiteDatabase db = mDbHelper.getWritableDatabase();
				Log.d("insertIntoSelf", "Inserting into DB for key: " + unhashedKey + " value: " + values);
				long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, cvObjectToInsert, SQLiteDatabase.CONFLICT_REPLACE);
				break;
			}
		}
	}

	public String localDumpToString () {

		FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = mDbHelper.getReadableDatabase();
		Cursor localDumpFromFunc = db.query(
				FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
				null,                               	  // The columns to return
				null,                                     // The columns for the WHERE clause
				null,                                     // The values for the WHERE clause
				null,                                     // don't group the rows
				null,                                     // don't filter by row groups
				null                                      // The sort order
		);

		String localDumpString = new String();

		if(localDumpFromFunc != null && localDumpFromFunc.getCount()>0 && localDumpFromFunc.moveToFirst()) {

			//if(localDumpFromFunc.moveToFirst()) {

			do {

				String key = localDumpFromFunc.getString(localDumpFromFunc.getColumnIndex(KEY_FIELD));
				String value = localDumpFromFunc.getString(localDumpFromFunc.getColumnIndex(VALUE_FIELD));
				localDumpString += key + "," + value + "$";

			} while (localDumpFromFunc.moveToNext());

			Log.d("Debug", "localDumpString: " + localDumpString);
			localDumpString = localDumpString.substring(0, localDumpString.length() - 1);
		}

		return localDumpString;
	}

	public String singleKeyFetchToString(String keyToFetch) {

		FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = mDbHelper.getReadableDatabase();
		Log.d("TestModule", "singleKeyFetchToString: Querying for key: " + keyToFetch);
		String[] selectionArray = new String[1];
		selectionArray[0] = keyToFetch;
		Cursor cursorObjectKeyFunc = db.query(
				FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
				null,                               // The columns to return
				"key=?",                                  // The columns for the WHERE clause
				selectionArray,                            // The values for the WHERE clause
				null,                                     // don't group the rows
				null,                                     // don't filter by row groups
				null                                 // The sort order
		);
		cursorObjectKeyFunc.moveToFirst();
		Log.d("TestModule", "singleKeyFetchToString: Cursor object called for key: " + keyToFetch);
		Log.d("TestModule", "singleKeyFetchToString: Size of the cursor object is: "+ cursorObjectKeyFunc.getCount());
		cursorObjectKeyFunc.moveToFirst();
		String key = cursorObjectKeyFunc.getString(cursorObjectKeyFunc.getColumnIndex(KEY_FIELD));
		String value = cursorObjectKeyFunc.getString(cursorObjectKeyFunc.getColumnIndex(VALUE_FIELD));
		String keyValueString = key + "$" + value;
		Log.d("TestModule", "singleKeyFetchToString: Inside key function, fetched key: " + key + " == " + keyToFetch + " which is " + keyValueString);
		return keyValueString;

	}

	public void StringToGlobalDump(String keyValuePairsString) {

		//Populate the strings into global matrix cursor
		if(keyValuePairsString != null) {
			String keyValuePairsArray[] = keyValuePairsString.split("\\$");
			for (String keyValuePair : keyValuePairsArray) {
				String keyAndValue[] = keyValuePair.split("\\,");
				matrixCursorGlobal.addRow(new String[]{keyAndValue[0], keyAndValue[1]});
			}
		}
	}

	public int StringToKeyMatrix(String keyValuePairString) {

		//Populate the strings into key matrix cursor
		Log.d("TestModule", "StringToKeyMatrix: Got String: " + keyValuePairString);
		String keyAndValue[] = keyValuePairString.split("\\$");
		matrixCursorKey = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
		matrixCursorKey.addRow(new String[]{keyAndValue[0], keyAndValue[1]});
		Log.d("TestModule", "StringToKeyMatrix: Size of the matrixCursorKey is: " + matrixCursorKey.getCount());
		matrixCursorKey.moveToFirst();
		String key = matrixCursorKey.getString(matrixCursorKey.getColumnIndex(KEY_FIELD));
		String value = matrixCursorKey.getString(matrixCursorKey.getColumnIndex(VALUE_FIELD));
		Log.d("TestModule", "StringToKeyMatrix: matrixCursorKey.KEY_FIELD: " + key + "   matrixCursorValue.VALUE_FIELD: " + value);
		return 0;

	}


	public boolean belongsToMe(String unhashedKey) {  //Checks if the key has any reason to be in this node

		int ownerPortNumber = findPartion(callHashFunction(unhashedKey));
		if(ownerPortNumber == node.selfPortNumber || ownerPortNumber == node.predecessor || ownerPortNumber == node.secondPredecessor) {
			return true;
		}
		return false;
	}

	public int handleFailureAndRecover(String[] keyValueFromNeighboursForRecovery) {

		for(String oneNeighbourDetails : keyValueFromNeighboursForRecovery) {

			if (oneNeighbourDetails!=null && oneNeighbourDetails.length() > 0){

				Log.d("Debug", "keyValueFromNeighboursForRecovery: " + oneNeighbourDetails);
				Log.d("Recovery", "Entered handleFailureAndRecover");
				String keyAndValuePairs[] = oneNeighbourDetails.split("\\$");
				Log.d("Recovery", "Split all the key-value pairs from single string to string[]");

				for (String singleKeyAndValuePair : keyAndValuePairs) {
					if (singleKeyAndValuePair != null) {
						String keyAndValue[] = singleKeyAndValuePair.split("\\,");
						Log.d("Debug", "singleKeyAndValuePair: " + singleKeyAndValuePair);
						if (belongsToMe(keyAndValue[0])) {

							//TODO: Possible error is if predecessor and successor have different versions of the same key. Then the system will not know which is latest and update the successor's value always
							Log.d("Recovery", "Going to update key: " + keyAndValue[0] + " with value: " + keyAndValue[1]);
							FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
							SQLiteDatabase db = mDbHelper.getReadableDatabase();

							ContentValues cvObjectToUpdate = new ContentValues();
							cvObjectToUpdate.put(KEY_FIELD, keyAndValue[0]);
							cvObjectToUpdate.put(VALUE_FIELD, keyAndValue[1]);

							long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, cvObjectToUpdate, SQLiteDatabase.CONFLICT_REPLACE);

						}
					}
				}
			}
		}

		return 1;

	}

	public void requestSuccessorForFailure(String msgToSend, NodeDetails ownerNode) {

		try {
			Log.d("TestModule", "Inside requestSuccessorForFailure");
			Socket socketToSuccessorForFailure = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), ownerNode.successor * 2);
			Log.d("TestModule", "Opened socket to " + ownerNode.successor*2 + " for " + msgToSend);
			BufferedWriter writeToSuccessorForFailure = new BufferedWriter(new OutputStreamWriter(socketToSuccessorForFailure.getOutputStream()));
			writeToSuccessorForFailure.write(msgToSend + "\n");
			writeToSuccessorForFailure.flush();

			BufferedReader readFromSuccessorForFailure = new BufferedReader(new InputStreamReader(socketToSuccessorForFailure.getInputStream()));
			String keySearchResult = readFromSuccessorForFailure.readLine();
			Log.d("TestModule", "ClientTask: Received String: " + keySearchResult);
			int noUseReturnInt = StringToKeyMatrix(keySearchResult);
			//keySearchStatus = true;
			Log.d("TestModule", "ClientTask: keySearchStatus is true and matrixCursorKey size is: " + matrixCursorKey.getCount());
			socketToSuccessorForFailure.close();

		} catch(IOException e) {
			Log.d("Failure", "IOException at requestSuccessorForFailure");
		}
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		String serverTaskTag = "ServerTask";

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					//Received message is broken to see what the status of the message is
					Log.d(serverTaskTag, "Server has started");

					Socket socket = serverSocket.accept();
					Log.d(serverTaskTag, "socket.isConnected(): " + socket.isConnected());

					BufferedReader serverReads = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					Log.d(serverTaskTag, "Server BufferedReader is initiated");

					String incomingMessage = serverReads.readLine();
					Log.d(serverTaskTag, "Server BufferedReader received message: " + incomingMessage);

					String[] splitMessage = incomingMessage.split("\\$");
					Log.d(serverTaskTag, "Received message " + incomingMessage + " is split");

					//socket.close();
					//Log.d(serverTaskTag, "socket.isClosed(): " + socket.isClosed());

					int switchCase = Integer.parseInt(splitMessage[0]);
					Log.d(serverTaskTag, "Switch element found is " + switchCase);

					switch(switchCase) {


						case 2:
							//Handle for when the insertion of messages are underway
							Log.d(serverTaskTag, "Switch element found is 2, for key: " + splitMessage[1]);
							insertIntoSelf(incomingMessage);
							socket.close();
							break;

						case 3:
							//Request of local dump for some server, Take the request and generate the localdumpstring and return to the requesting node
							if(Integer.parseInt(splitMessage[1]) != node.selfPortNumber) {

								String keyValuesToInsert = localDumpToString();
								//Socket socketToGlobalRequestingNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(splitMessage[1])*2);
								BufferedWriter writeToSelfForGlobalDump = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
								writeToSelfForGlobalDump.write(keyValuesToInsert+ "\n");
								writeToSelfForGlobalDump.flush();
								//socket.close();
								Log.d("ClientTask", "Sent Message: " + keyValuesToInsert + " to " + node.selfPortNumber + " and socket.isClose() status is: " + socket.isClosed());
							}

							else {

								String keyValuesToInsert = localDumpToString();
								//Populate the strings to global matrix cursor
								StringToGlobalDump(keyValuesToInsert);

							}
							break;

						case 4:
							//Return key search result
							Log.d("TestModule", "ServerTask: Request for key: " + splitMessage[2]);
							String singleKeyValuePair = singleKeyFetchToString(splitMessage[2]);
							//Socket socketToKeyRequestingNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(splitMessage[1])*2);
							Log.d("TestModule", "ServerTask: Socket is opened to: " + splitMessage[1]);
							BufferedWriter writeToSelfForKeyRequest = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

							writeToSelfForKeyRequest.write(singleKeyValuePair + "\n");
							writeToSelfForKeyRequest.flush();
							//socket.close();
							break;
					}



				}
			}
			catch(IOException e) {
				Log.d(serverTaskTag, "Server IOException");
			}

			return null;

		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		String clientTaskTag = "ClientTask";
		@Override
		protected Void doInBackground(String... msgs) {

			String msgToSend = msgs[0];
			String[] msgParts = msgToSend.split("\\$");
			Log.d(clientTaskTag, "Received Message: " + msgToSend);
			try {
				switch(Integer.parseInt(msgParts[0])) {

					case 0:	//Ping your node.predecessor and node.successor for their key and values db so that you can recover, Also handle null if their DB's are empty

						Log.d("Recovery", "ClientTask: Handling Recovery!");
						ArrayList<Integer> listNeighbours = new ArrayList<Integer>();
						int count = 0;
						String[] keyValueFromNeighboursForRecovery = new String[2];
						listNeighbours.add(node.predecessor);
						listNeighbours.add(node.successor);

						for(int neighbourPort : listNeighbours) {

							Socket socketToNeighbourForRecovery = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), neighbourPort * 2);
							BufferedWriter writeToNeighbourForRecovery = new BufferedWriter(new OutputStreamWriter(socketToNeighbourForRecovery.getOutputStream()));
							msgToSend = 3 + "$" + node.selfPortNumber;
							writeToNeighbourForRecovery.write(msgToSend + "\n");
							writeToNeighbourForRecovery.flush();

							BufferedReader readFromNeighbourForRecovery = new BufferedReader(new InputStreamReader(socketToNeighbourForRecovery.getInputStream()));

							keyValueFromNeighboursForRecovery[count] = readFromNeighbourForRecovery.readLine();
							count++;

						}
						if( (keyValueFromNeighboursForRecovery[0] != null && !keyValueFromNeighboursForRecovery[0].isEmpty())
								&&
								(keyValueFromNeighboursForRecovery[1] != null && !keyValueFromNeighboursForRecovery[1].isEmpty()) ) {

							int status = handleFailureAndRecover(keyValueFromNeighboursForRecovery); //Will receive 1 when operation is complete

						}
						break;

					case 1: //Sends to own or self server

						Log.d(clientTaskTag, "Case 1 is executing at the client");

						NodeDetails tempQuorumNode = new NodeDetails(); //TODO: Check Java constructor calling
						//tempQuorumNode.selfPortNumber = Integer.parseInt(msgParts[1]);
						tempQuorumNode.NodeDetails(Integer.parseInt(msgParts[1]));
						Log.d(clientTaskTag, "NodeDetails instance was created for case 1: " + tempQuorumNode.selfPortNumber + "==" + msgParts[1]);

						String msgToSendToQuorum = 2 + "$" + msgParts[2] + "$" + msgParts[3];
						Log.d(clientTaskTag, "Message being sent to Quorum is: " + msgToSendToQuorum);


						if(!(tempQuorumNode.selfPortNumber == node.selfPortNumber)) {

							Log.d(clientTaskTag, "Inside if(tempQuorumNode.selfPortNumber != node.selfPortNumber)");
							Socket socketToOwner = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), tempQuorumNode.selfPortNumber*2);

							/*
							InetAddress addr = InetAddress.getByAddress(new byte[]{10, 0 , 2, 2});
							SocketAddress socketToOwnerAddress = new InetSocketAddress(addr, tempQuorumNode.selfPortNumber*2);
							Socket socketToOwner = new Socket();
							socketToOwner.connect(socketToOwnerAddress, 2000);
							*/

							Log.d(clientTaskTag, "Socket opened to " + tempQuorumNode.selfPortNumber*2);
							BufferedWriter writerToOwner = new BufferedWriter(new OutputStreamWriter(socketToOwner.getOutputStream()));
							writerToOwner.write(msgToSendToQuorum + "\n");
							writerToOwner.flush();
							//socketToOwner.close();

						}
						else
							insertIntoSelf(msgToSendToQuorum);

						if(!(tempQuorumNode.successor == node.selfPortNumber)) {
							Log.d(clientTaskTag, "Inside if(tempQuorumNode.selfPortNumber != node.successor)");
							Socket socketToSuccessor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), tempQuorumNode.successor*2);
							Log.d(clientTaskTag, "Socket opened to " + tempQuorumNode.successor*2);
							BufferedWriter writerToSuccessor = new BufferedWriter(new OutputStreamWriter(socketToSuccessor.getOutputStream()));
							writerToSuccessor.write(msgToSendToQuorum + "\n");
							writerToSuccessor.flush();
							//socketToSuccessor.close();
						}
						else
							insertIntoSelf(msgToSendToQuorum);

						if(!(tempQuorumNode.secondSuccessor == node.selfPortNumber)) {
							Log.d(clientTaskTag, "Inside if(tempQuorumNode.selfPortNumber != node.secondSuccessor)");
							Socket socketToSecondSuccessor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), tempQuorumNode.secondSuccessor*2);
							Log.d(clientTaskTag, "Socket opened to " + tempQuorumNode.secondSuccessor*2);
							BufferedWriter writerToSecondSuccessor = new BufferedWriter(new OutputStreamWriter(socketToSecondSuccessor.getOutputStream()));
							writerToSecondSuccessor.write(msgToSendToQuorum + "\n");
							writerToSecondSuccessor.flush();
							//socketToSecondSuccessor.close();
						}
						else
							insertIntoSelf(msgToSendToQuorum);

						break;

					/*
					Socket socketToSelf = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), node.selfPortNumber*2);
						BufferedWriter writeToStartNode = new BufferedWriter(new OutputStreamWriter(socketToSelf.getOutputStream()));
						writeToStartNode.write(msgToSend + "\n");
						writeToStartNode.flush();
						socketToSelf.close();
						Log.d("ClientTask", "Sent Message: " + msgToSend + " to " + node.selfPortNumber + " and socketToStartNode.isClose() status is: " + socketToSelf.isClosed());
						break;
					*/

					case 3: //Send to all nodes for global querying
						Log.d("ClientTask", "Case 3 is executing at the client");

						for(int portNumber : memberList) {

							//Send out request for all key value pairs for global dump
							Socket socketForGlobalDump = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNumber*2);
							BufferedWriter writeToAllForGlobalDump = new BufferedWriter(new OutputStreamWriter(socketForGlobalDump.getOutputStream()));
							writeToAllForGlobalDump.write(msgToSend + "\n");
							writeToAllForGlobalDump.flush();

							if(portNumber != node.selfPortNumber) {
								//Read all key value pairs for global dump
								BufferedReader readForGlobalDump = new BufferedReader(new InputStreamReader(socketForGlobalDump.getInputStream()));
								String keyValuesFromOtherNode = readForGlobalDump.readLine();
								if (keyValuesFromOtherNode != null) {
									StringToGlobalDump(keyValuesFromOtherNode);
								}
							}

							socketForGlobalDump.close();
							Log.d("ClientTask", "Sent Message: " + msgToSend + " to " + node.selfPortNumber + " and socketForGlobalDump.isClose() status is: " + socketForGlobalDump.isClosed());
						}
						//globalDumpStatus = true;
						break;

					case 4:

						//Handle key search
						Log.d("TestModule", "ClientTask: Inside case 4");
						int keyPortNumber = findPartion(callHashFunction(msgParts[2]));
						Log.d("TestModule", "ClientTask: Message: " + msgParts[2] + " goes into the node with port number: " + keyPortNumber);
						Socket socketForKey = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), keyPortNumber*2);

						NodeDetails tempKeyNode = new NodeDetails();
						tempKeyNode.NodeDetails(keyPortNumber);

						BufferedWriter writeToPartitionForKey = new BufferedWriter(new OutputStreamWriter(socketForKey.getOutputStream()));
						writeToPartitionForKey.write(msgToSend + "\n");
						writeToPartitionForKey.flush();

						BufferedReader readKeySearchString = new BufferedReader(new InputStreamReader(socketForKey.getInputStream()));
						String keySearchResult = readKeySearchString.readLine();
						if (keySearchResult != null) {
							Log.d("TestModule", "ClientTask: Received String: " + keySearchResult + " for key: " + msgParts[2]);

							int noUseReturnInt = StringToKeyMatrix(keySearchResult);
							//keySearchStatus = true;
							Log.d("TestModule", "ClientTask: keySearchStatus is true and matrixCursorKey size is: " + matrixCursorKey.getCount());
							socketForKey.close();
						}

						else {
							Log.d(clientTaskTag, "Inside Client Case 4, else");
							socketForKey.close();
							requestSuccessorForFailure(msgToSend, tempKeyNode);
						}
						break;


				}
			}
			catch(IOException e) {
				Log.d("ClientTask", "Client IOException!");
			}

			return null;

		}
	}

}


