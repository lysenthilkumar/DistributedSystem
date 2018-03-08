package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.graphics.SweepGradient;
import android.net.Uri;

//Self added imports
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;
import android.content.Context;
import android.util.Log;
import android.telephony.TelephonyManager;
import android.os.AsyncTask;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SimpleDhtProvider extends ContentProvider {

    //Global variables
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    boolean cvGlobalStatus = false;

    //Variables to identify this AVD
    int selfPortNumber = -1, predecessor = -1, successor = -1;
    String hashedSelfPortNumber = "NOTSET";
    String hashedPredecessor = "NOTSET";
    String hashedSuccessor = "NOTSET";

    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public String callHashFunction(String nodeID) {
        try {
            String hashedNodeID = genHash(nodeID);
            return hashedNodeID;
        } catch (NoSuchAlgorithmException e) {
            Log.d("Hash function call", "Just hashed: " + nodeID);
        }
        return null;
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

        if(selection.equals("*") || selection.equals("@")){
            rowsCount = db.delete("keyValueTable", null, null);
            Log.d("Delete", "rowCount after * or @: " + rowsCount);
            return rowsCount;
        }

        rowsCount = db.delete("keyValueTable", "key=?", new String[] {selection});
        Log.d("Delete", "First rowCount after key: " + selection +" is " + rowsCount);

        String updatedSelection = ":" + selection;
        rowsCount = db.delete("keyValueTable", "key=?", new String[] {updatedSelection});
        Log.d("Delete", "Second rowCount after key: " + selection +" is " + rowsCount);
        return rowsCount;

    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.d("CASE4", "Inside insert()");
        //Split Content values object to find the key to check for if condition
        String newMessageKey = String.valueOf(values.get(KEY_FIELD));
        String hashedNewMessageKey = callHashFunction(newMessageKey);
        String newMessageValue = String.valueOf(values.get(VALUE_FIELD));

        Log.d("CASE4", "Message being pushed is KEY: " + newMessageKey + " VALUE: " + newMessageValue );

        if (cvGlobalStatus) {

            Log.d("CASE4", "Inside if(cvGlobalStatus)");
            cvGlobalStatus = false;
            Log.d("cvGlobalStatus if", "Insertion is enabled at this AVD!");
            FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);

        }

        else {

            Log.d("CASE4", "Insert: Message being pushed directly into " + selfPortNumber);

            //Single node in existance or (greater than predecessor and less than self)
            if ((hashedSuccessor.equals("NOTSET") || hashedPredecessor.equals(hashedSelfPortNumber))
                    ||
                    (hashedPredecessor.compareTo(hashedNewMessageKey) < 0 && hashedNewMessageKey.compareTo(hashedSelfPortNumber) <= 0)) {

                //Broadcast

                Log.d("CASE4", "if(){}");
                FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                //Log.d("Insert function: ", String.valueOf(newRowId));
                //Log.d("insert", values.toString());

                if (!(hashedSuccessor.compareTo("NOTSET") == 0 || hashedPredecessor.equals(hashedSelfPortNumber))) {
                    Log.d("QueryBlock", "Inside !If-------------------");
                    String messageToAllMembers = 4 + "$" + newMessageKey + "$" + newMessageValue;
                    String updatedKey = ":" + newMessageKey;
                    ContentValues updatedCV = new ContentValues();
                    updatedCV.put(KEY_FIELD, updatedKey);
                    updatedCV.put(VALUE_FIELD, newMessageValue);
                    long updatedUri = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, updatedCV, SQLiteDatabase.CONFLICT_REPLACE);

                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToAllMembers);

                }

                return uri;
            }

            //predecessor is greater than self and (message is greater than predecessor or message is less than or equal to self)
            else if ((hashedSelfPortNumber.compareTo(hashedPredecessor) < 0)
                    &&
                    (hashedPredecessor.compareTo(hashedNewMessageKey) < 0 || hashedNewMessageKey.compareTo(hashedSelfPortNumber) <= 0)) {

                //Broadcast

                Log.d("Insert: else if", "Insertion is enabled at this AVD!");
                FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                long newRowId = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                //Log.d("Insert function: ", String.valueOf(newRowId));
                //Log.d("insert", values.toString());

                String messageToAllMembers = 4 + "$" + newMessageKey + "$" + newMessageValue;
                String updatedKey = ":" + newMessageKey;
                ContentValues updatedCV = new ContentValues();
                updatedCV.put(KEY_FIELD, updatedKey);
                updatedCV.put(VALUE_FIELD, newMessageValue);
                long updatedUri = db.insertWithOnConflict(FeedReaderContract.FeedEntry.TABLE_NAME, null, updatedCV, SQLiteDatabase.CONFLICT_REPLACE);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToAllMembers);

                return uri;
            }

            //pass along the ring
            else {

                Log.d("Insert: else", "Message being passed to client");
                String cvUnhashedStringToClient = "3" + "$" + values.get(KEY_FIELD) + "$" + values.get(VALUE_FIELD);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, cvUnhashedStringToClient);

            }
        }

        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        //TelephonyManager to set port numbers to different avd's
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        selfPortNumber = (Integer.parseInt(portStr)); //Find the port number of the AVD will be port number * 2
        hashedSelfPortNumber = callHashFunction(String.valueOf(selfPortNumber));

        Log.d("OnCreate: ", "This AVD's Port number is: " + selfPortNumber);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d("OnCreate", "This AVD's Server is launching");
        } catch (IOException e) {
            Log.e("OnCreate", "Can't create a ServerSocket");
        }

        if (selfPortNumber == 5554) {

            successor = 5554;
            hashedSuccessor = callHashFunction(String.valueOf(successor));

            predecessor = 5554;
            hashedPredecessor = callHashFunction(String.valueOf(predecessor));

            Log.d("OnCreate", "Order of the AVD at the start for " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);


        }

        else {

            Log.d("OnCreate", "This AVD is " + selfPortNumber + " and its calling its client function");
            String messageToClientAtStart =  "-1" + "$" + selfPortNumber;
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, messageToClientAtStart);
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        FeedReaderDbHelper mDbHelper = new FeedReaderDbHelper(getContext());
        SQLiteDatabase db = mDbHelper.getReadableDatabase();
        String[] selectionArray = new String[1];
        selectionArray[0] = selection;

        if ( hashedSuccessor.compareTo("NOTSET") == 0 || hashedPredecessor.equals(hashedSelfPortNumber) ) {

            if (selection.equals("*") || selection.equals("@")) {
                Cursor cursorObjectAll = db.query(
                        FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
                        null,                                     // The columns to return
                        null,                                     // The columns for the WHERE clause
                        null,                                     // The values for the WHERE clause
                        null,                                     // don't group the rows
                        null,                                     // don't filter by row groups
                        null                                      // The sort order
                );
                Log.v("query", selectionArray[0]);
                return cursorObjectAll;
            }

            else {

                Log.d("QueryBlock else", "Request to submit message with key: " + selectionArray[0]);

                Cursor cursorObject = db.query(
                        FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
                        null,                               // The columns to return
                        "key=?",                                  // The columns for the WHERE clause
                        selectionArray,                            // The values for the WHERE clause
                        null,                                     // don't group the rows
                        null,                                     // don't filter by row groups
                        null                                 // The sort order
                );
                Log.v("query", selectionArray[0]);
                return cursorObject;
            }


        }

        else {

            Log.d("QueryBlock", "Inside else handling more than one node");
            Cursor cursorObjectAll = db.query(
                    FeedReaderContract.FeedEntry.TABLE_NAME,  // The table to query
                    null,                               // The columns to return
                    null,                                  // The columns for the WHERE clause
                    null,                            // The values for the WHERE clause
                    null,                                     // don't group the rows
                    null,                                     // don't filter by row groups
                    null                                 // The sort order
            );

            Log.v("QueryBlock", "Cursor object populated");

            MatrixCursor matrixCursorLocal = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            MatrixCursor matrixCursorGlobal = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            MatrixCursor matrixCursorKey = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});

            cursorObjectAll.moveToFirst();

            do {

                //int keyIndex = cursorObjectAll.getColumnIndex(KEY_FIELD);
                //int valueIndex = cursorObjectAll.getColumnIndex(VALUE_FIELD);

                //Log.d("QueryBlock", "Key Index: " + keyIndex + " Value Index: " + valueIndex);

                Log.d("QueryBlock Highlight", "Size of cursor: " + cursorObjectAll.getCount());

                String key = cursorObjectAll.getString(cursorObjectAll.getColumnIndex(KEY_FIELD));
                String value = cursorObjectAll.getString(cursorObjectAll.getColumnIndex(VALUE_FIELD));

                Log.d("QueryBlock", "Key found: " + key + " Value found: " + value);

                if (key.startsWith(":")) {

                    Log.d("QueryBlock", "Inside if(key.startsWith(:))");

                    String actualKey = key.substring(1);
                    matrixCursorGlobal.addRow(new String[]{actualKey, value});

                    Log.d("QueryBlock", "Populated global dump and size is: " + matrixCursorGlobal.getCount());

                    if (!selection.equals("*") && !selection.equals("@")) {

                        if (selection.equals(actualKey)) {

                            Log.d("QueryBlock", "Inside if(selection.equals(actualKey)) ");

                            matrixCursorKey.addRow(new String[]{actualKey, value});

                            Log.d("QueryBlock", "Populated key dump and size is: " + matrixCursorKey.getCount());

                            return matrixCursorKey;

                        }
                    }
                }

                else {

                    matrixCursorLocal.addRow(new String[]{key, value});
                    Log.d("QueryBlock", "Populated local dump and size is: " + matrixCursorLocal.getCount());

                }

            } while (cursorObjectAll.moveToNext());

            if (selection.equals("*")) {

                return matrixCursorGlobal;

            }

            else if(selection.equals("@"))

                return matrixCursorLocal;

            return null;
        }
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

                    if(Integer.parseInt(splitMessage[0]) == 4) {

                        BufferedWriter bwNextSuccesor = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        String nextSuccessor = String.valueOf(successor);
                        bwNextSuccesor.write(nextSuccessor + "\n");
                        bwNextSuccesor.flush();

                    }


                    socket.close();
                    Log.d(serverTaskTag, "socket.isClosed(): " + socket.isClosed());

                    int switchCase = Integer.parseInt(splitMessage[0]);
                    Log.d(serverTaskTag, "Switch element found is " + switchCase);

                    Log.d(serverTaskTag, "Server of " + selfPortNumber + " will run switch { if and else if } for " + splitMessage[1]);


                    if( ((hashedSuccessor).compareTo(callHashFunction(splitMessage[1])) > 0) ) {
                        Log.d("Checking else if", "Seeker node " + splitMessage[1] + " is less than " + selfPortNumber + "'s successor " + successor);
                        if( ((hashedSelfPortNumber).compareTo(callHashFunction(splitMessage[1])) < 0) ) {
                            Log.d("Checking else if", "Seeker node " + splitMessage[1] + " is greater than " + selfPortNumber  + "  So it should satisfy the else if clause");
                        }
                    }


                    switch (switchCase) {
                        //Need to place a seeker node into the chord
                        case -1:

                            //Second node to join the chord
                            if(hashedSuccessor.equals(hashedSelfPortNumber)) {

                                Log.d(serverTaskTag, "Second node joining");

                                //Inform seeker about his new successor and predecessor which is self
                                Socket socketToSecondNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(splitMessage[1])*2);
                                Log.d(serverTaskTag, "socketToSecondNode.isConnected() status is: " + socketToSecondNode.isConnected());
                                BufferedWriter serverSaysToSecondNode = new BufferedWriter(new OutputStreamWriter(socketToSecondNode.getOutputStream()));
                                String updateSecondNode = 1 + "$" + selfPortNumber + "$" + selfPortNumber;  //Updating seeker node details
                                serverSaysToSecondNode.write(updateSecondNode + "\n");
                                serverSaysToSecondNode.flush();
                                Log.d(serverTaskTag, "Server of " + selfPortNumber + " is sending " + updateSecondNode + " to " + Integer.parseInt(splitMessage[1]));
                                socketToSecondNode.close();
                                Log.d(serverTaskTag, "socketToSecondNode.isClosed() status is: " + socketToSecondNode.isClosed());

                                //Update your own successor and predecessor as joining node
                                predecessor = Integer.parseInt(splitMessage[1]);
                                hashedPredecessor = callHashFunction(splitMessage[1]);
                                successor =  Integer.parseInt(splitMessage[1]);
                                hashedSuccessor = callHashFunction(splitMessage[1]);

                                Log.d("ServerTask", "Order of the AVD " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);
                            }

                            //Node join somewhere in between
                            else if(  ((hashedSuccessor).compareTo(callHashFunction(splitMessage[1])) > 0)
                                    &&
                                    ((hashedSelfPortNumber).compareTo(callHashFunction(splitMessage[1])) < 0)) {

                                Log.d(serverTaskTag, "Middle node entry code executing");

                                //Inform successor about the seeker node
                                Socket socketToSuccessor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor*2);
                                //Log.d(serverTaskTag, "Server starts a socketToSuccessor with: " + successor);
                                BufferedWriter serverSaysToSuccessor = new BufferedWriter(new OutputStreamWriter(socketToSuccessor.getOutputStream()));
                                //Log.d(serverTaskTag, "Server initiates  serverSays as a BufferedWriter object");
                                String updateYourPredecessor = 2 + "$" + splitMessage[1]; //Updating predecessor of self's successor
                                //Log.d(serverTaskTag, "Message being sent to successor is: " + updateYourPredecessor);
                                serverSaysToSuccessor.write(updateYourPredecessor + "\n");
                                serverSaysToSuccessor.flush();
                                //Log.d(serverTaskTag, "Message has been sent: " + updateYourPredecessor);
                                socketToSuccessor.close();
                                //Log.d(serverTaskTag, "Server stops a socketToSuccessor with: " + successor);


                                //Inform the seeker node about his position
                                Socket socketToSeeker = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(splitMessage[1])*2);
                                //Log.d(serverTaskTag, "Server starts a socketToSeeker with: " + splitMessage[1]);
                                BufferedWriter serverSaysToSeeker = new BufferedWriter(new OutputStreamWriter(socketToSeeker.getOutputStream()));
                                //Log.d(serverTaskTag, "Server initiates  serverSays as a BufferedWriter object");
                                String updateYourPredecessorSuccessor = 1 + "$" + selfPortNumber + "$" + successor;  //Updating seeker node details
                                serverSaysToSeeker.write(updateYourPredecessorSuccessor + "\n");
                                serverSaysToSeeker.flush();
                                socketToSeeker.close();

                                //Update my own successor
                                successor = Integer.parseInt(splitMessage[1]);
                                hashedSuccessor = callHashFunction(splitMessage[1]);

                                Log.d("ServerTask", "Order of the AVD " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);

                            }

                            //For end of the chord fragment
                            else if (
                                    ((hashedPredecessor.compareTo(hashedSelfPortNumber)) < 0
                                            &&
                                            (hashedSuccessor.compareTo(hashedSelfPortNumber)) < 0)
                                            &&
                                            ((callHashFunction(splitMessage[1]).compareTo(hashedSuccessor)) < 0
                                                    ||
                                                    (callHashFunction(splitMessage[1]).compareTo(hashedSelfPortNumber)) > 0 )
                                    )
                            {
                                //When seeker node is greater than me or lesser than my successor

                                Log.d(serverTaskTag, "End of chord else if executing");

                                //if (Integer.parseInt(splitMessage[1]) < successor || Integer.parseInt(splitMessage[1]) > selfPortNumber) {
                                //Inform successor about seeker node joining and update his predecessor
                                Socket socketToSmallestNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor*2);
                                BufferedWriter serverSaysToSmallestNode = new BufferedWriter(new OutputStreamWriter(socketToSmallestNode.getOutputStream()));
                                String updateYourPredecessorNewEnd = 2 + "$" + splitMessage[1];  //Updating predecessor of self's successor
                                serverSaysToSmallestNode.write(updateYourPredecessorNewEnd + "\n");
                                serverSaysToSmallestNode.flush();
                                socketToSmallestNode.close();

                                //Inform seeker node about his position in the chord
                                Socket socketToSeekerToFinishChord = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(splitMessage[1])*2);
                                BufferedWriter serverSaysToSeekerNewEnd = new BufferedWriter(new OutputStreamWriter(socketToSeekerToFinishChord.getOutputStream()));
                                String updateNewEndChord = 1 + "$" + selfPortNumber + "$" + successor;  //Updating seeker node details
                                serverSaysToSeekerNewEnd.write(updateNewEndChord + "\n");
                                serverSaysToSeekerNewEnd.flush();
                                socketToSeekerToFinishChord.close();

                                //Update this AVD's successor
                                successor = Integer.parseInt(splitMessage[1]);
                                hashedSuccessor = callHashFunction(String.valueOf(successor));

                                Log.d("ServerTask", "Order of the AVD " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);

                                //}
                            }

                            //Pass the seeker node to next node
                            else {

                                Log.d(serverTaskTag, "Passing code executing");

                                //Open connection to successor and pass the message directly
                                Log.d("ServerTask", "This AVD " + selfPortNumber + " is forwarding the message " + incomingMessage + " to " + successor);
                                Socket socketToPass = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor*2);
                                BufferedWriter writerToPass = new BufferedWriter(new OutputStreamWriter(socketToPass.getOutputStream()));
                                writerToPass.write(incomingMessage + "\n");
                                writerToPass.flush();
                                socketToPass.close();
                            }
                            break;

                        case 1:

                            //Successful node join update
                            predecessor = Integer.parseInt(splitMessage[1]);
                            hashedPredecessor = callHashFunction(splitMessage[1]);
                            successor = Integer.parseInt(splitMessage[2]);
                            hashedSuccessor = callHashFunction(splitMessage[2]);

                            Log.d("ServerTask", "Order of the AVD " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);

                            break;

                        case 2:

                            //This AVD's predecessor telling me to update my own predecessor
                            predecessor = Integer.parseInt(splitMessage[1]);
                            hashedPredecessor = callHashFunction(splitMessage[1]);

                            Log.d("ServerTask", "Order of the AVD " + selfPortNumber + " is: " + predecessor + "->" + selfPortNumber + "->" + successor);

                            break;

                        case 3:

                            //Handle for when the insertion of messages are underway
                            Log.d(serverTaskTag, "Insertion Server code running");
                            String unhashedKeyCV = splitMessage[1];
                            String valuesOrMessageCV = splitMessage[2];
                            ContentValues cvObjectToInsert = new ContentValues();
                            cvObjectToInsert.put(KEY_FIELD, unhashedKeyCV);
                            cvObjectToInsert.put(VALUE_FIELD, valuesOrMessageCV);
                            Uri newuri =  getContext().getContentResolver().insert(mUri, cvObjectToInsert);
                            break;

                        case 4:

                            Log.d("CASE4", "ServerTask: Inside case 4");
                            ContentValues cvGlobal = new ContentValues();
                            String updatedKeys = ":" + splitMessage[1];
                            cvGlobal.put(KEY_FIELD, updatedKeys);
                            cvGlobal.put(VALUE_FIELD, splitMessage[2]);
                            cvGlobalStatus = true;
                            Uri cvUri =  getContext().getContentResolver().insert(mUri, cvGlobal);
                            break;


                    }




                }
            } catch (IOException e) {
                System.out.println("I/O Exception thrown at Server!");
            }
            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            String[] msgParts = msgToSend.split("\\$");
            Log.d("ClientTask", "Received Message: "+msgToSend);
            try {

                switch(Integer.parseInt(msgParts[0])) {

                    case -1:
                        Socket socketToStartNode = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                        BufferedWriter writeToStartNode = new BufferedWriter(new OutputStreamWriter(socketToStartNode.getOutputStream()));
                        writeToStartNode.write(msgToSend + "\n");
                        writeToStartNode.flush();
                        socketToStartNode.close();
                        Log.d("ClientTask", "Sent Message: " + msgToSend + " to 5554 and socketToStartNode.isClose() status is: " + socketToStartNode.isClosed());
                        break;

                    case 3:

                        Log.d("ClientTask", "Passing message to: " + successor);
                        Socket socketToSuccessor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), successor*2);
                        BufferedWriter writeToSuccessor = new BufferedWriter(new OutputStreamWriter(socketToSuccessor.getOutputStream()));
                        writeToSuccessor.write(msgToSend + "\n");
                        writeToSuccessor.flush();
                        socketToSuccessor.close();
                        break;

                    case 4:
                        Log.d("CASE4", "ClientTask: Inside case 4");
                        int chordSuccessor = successor;

                        while (chordSuccessor != selfPortNumber) {

                            Log.d("CASE4", "Inside while(chordSuccesor != selfPortNumer){} Informing: " + chordSuccessor);
                            Socket socketToSuccesor = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), chordSuccessor*2);
                            BufferedWriter bwToSuccessor = new BufferedWriter(new OutputStreamWriter(socketToSuccesor.getOutputStream()));
                            bwToSuccessor.write(msgToSend + "\n");
                            bwToSuccessor.flush();

                            BufferedReader brFromSuccesor = new BufferedReader(new InputStreamReader(socketToSuccesor.getInputStream()));
                            String tempSuccesor = brFromSuccesor.readLine();
                            Log.d("CASE4", "tempSuccessor: " + tempSuccesor);
                            chordSuccessor = Integer.parseInt(tempSuccesor);
                            Log.d("CASE4", "Hearing from previous dude about next successor: " + chordSuccessor);
                            socketToSuccesor.close();

                        }

                        break;
                }

            } catch(UnknownHostException e) {
                Log.d("ClientTask", "UnknownHostException thrown at client: "+e.toString());
            } catch (IOException e) {
                Log.e("ClientTask", "IOException thrown at client: "+e.toString());
            }
            return null;
        }
    }
}
