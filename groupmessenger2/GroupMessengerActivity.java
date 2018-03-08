package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
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

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.acl.Group;
import java.lang.String;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/*

customQueueObject - Name of the custom class by definition
customQObj - Name of the instance of customQueueObject
pqObj - Instance of a Priority Queue of type <customQueueObject>

*/

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 *
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    public class customQueueObject {

        String queueMessage;      //Message
        //int messageID;            //Message ID of message
        int senderPortNumber;     //Sender AVD's port number
        int serverSeqNum;         //Server proposes priority or the agreed priority
        int APServerNumber;       //Server number of the avd that decides the agreed priority
        String status;            //Status Deliverable/Undeliverable

        public int compareMsgs (customQueueObject msg1) {   //Returns the message with the lower priority
            if(this.serverSeqNum > msg1.serverSeqNum)
                return -1;
            else if(this.serverSeqNum < msg1.serverSeqNum)
                return 1;
            else {
                if(this.status == "UNDELIVERABLE" && msg1.status == "DELIVERABLE")
                    return 1;
                else if (this.status == "DELIVERABLE" && msg1.status == "UNDELIVERABLE")
                    return -1;
                else {
                    if (this.APServerNumber > msg1.APServerNumber)
                        return -1;
                    else
                        return 1;
                }
            }
        }
        public void printObject(customQueueObject messageDetails) {
            System.out.println("Message is: "+messageDetails.queueMessage);
            //System.out.println("Message ID is: "+messageDetails.messageID);
        }
    }
    //customQueueObject customQObj = new customQueueObject();

    class msgComparator implements Comparator<customQueueObject> {   //Calling compareMsgs
        public int compare(customQueueObject msg1, customQueueObject msg2) {
            return msg2.compareMsgs(msg1);
        }
    }

    //Global variables
    int DBcounter = 0, seqNum = -1, failedAVDNumber = -1;
    String globalPortNumber = new String(); //Holds the port number as unique device ID

    PriorityQueue<customQueueObject> pqObj = new PriorityQueue<customQueueObject>(10, new msgComparator());

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    //Assigning port numbers to the avd's
    static final String[] REMOTE_PORT = {"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.groupmessenger2.provider");
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        //TelephonyManager to set port numbers to different avd's
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        globalPortNumber = myPort;

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        final EditText editText = (EditText) findViewById(R.id.editText1);

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
        findViewById(R.id.button4).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box
                //localTextView.append("\t" + msg); // This is one way to display a string.
                TextView remoteTextView = (TextView) findViewById(R.id.textView1);
                remoteTextView.append("\n");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            try {
                while (true) {

                    Socket socket = serverSocket.accept();
                    //int inQueue = 0; //Holds the status for message in queue 1; for message not in queue 0
                    BufferedReader serverReads = new BufferedReader(new InputStreamReader(socket.getInputStream())); //Takes message from client
                    String serverRecMsg = serverReads.readLine(); //Message from client is read
                    //System.out.println("Server receives from client : "+serverRecMsg);

                    if(serverRecMsg == null)  //If the message received is null then break the server run for that avd
                        break;

                    Log.d(TAG, "II Server receives from client : "+serverRecMsg);

                    String[] msgFragments = serverRecMsg.split("\\$"); //Split incoming message $

                    //Separate the fragments into independent parts
                    String incomingMessage = msgFragments[0];
                    int msgID = Integer.parseInt(msgFragments[1]);  //dummyPriority or Agreed Priority
                    String senderProcessID = msgFragments[2];  //Port number of process that sends the message
                    int APServerID = Integer.parseInt(msgFragments[3]);  //Server number of the AVD that chooses the agreed priority

                    //If the failedAVDNumber is received from another AVD then
                    if(failedAVDNumber == -1)
                        failedAVDNumber = Integer.parseInt(msgFragments[4]);

                    int phase = Integer.parseInt(msgFragments[5]);  //Used to differentiate between new messages or old messages

                    /*
                    TODO: Instructions to self
                    Larger than agreed priority is what is sent by client after deciding
                    Larger than self priority means that they should not have proposed any priority
                    Implement Priority Queue
                    Build an object of customQueueObject
                    Add to the queue and order by one parameter

                    if in queue, extract the message as customQueueObject and update priority and status and push into queue
                    else if not in queue add to queue
                    */

                    if(phase == 1) {

                        //Check for elements in queue
                        if(seqNum < msgID) //Agreed priority is replaced on seqNum
                            seqNum = msgID;

                        /*
                        //Checks if arrangment in queue holds the minimum at the top
                        Iterator<customQueueObject> itr1 = pqObj.iterator();

                        customQueueObject printObject = pqObj.peek();
                        int minValue = printObject.serverSeqNum;
                        while(itr1.hasNext()) {
                            System.out.println("Minimum value found at the top is: " + printObject.serverSeqNum);
                            printObject = itr1.next();
                            if(minValue > printObject.serverSeqNum)
                                System.out.println("Found minimum element lower down the queue!");
                        }
                        */

                        Iterator<customQueueObject> itr = pqObj.iterator();

                        while(itr.hasNext()) {
                            customQueueObject customQObj = itr.next();
                            if (customQObj.queueMessage.equals(incomingMessage) && customQObj.senderPortNumber == Integer.parseInt(senderProcessID)) {

                                pqObj.remove(customQObj); //This is the message to update with agreed priority
                                //if(pqObj.contains(customQObj))
                                    //System.out.println("---------------------------------------Some Problem With Insertion!--------------------------------------------");
                                customQObj.serverSeqNum = msgID; //Holds agreed priority
                                customQObj.APServerNumber = APServerID; //Server Number that suggested the agreed priority
                                customQObj.status = "DELIVERABLE"; //Status is updated
                                pqObj.add(customQObj); //Message to be update is again inserted into the PQ
                                //customQObj.printObject(customQObj); //Printing the object
                                break;
                            }
                        }

                        //Check and remove if the object in the queue is
                        customQueueObject customQObjCheck = pqObj.peek();
                        if(customQObjCheck.status == "UNDELIVERABLE" &&  customQObjCheck.senderPortNumber == failedAVDNumber) {
                            pqObj.remove(customQObjCheck);
                            System.out.println("Some problem with phase 1!");
                        }

                        //Insert messages into database and remove from priority queue
                        customQueueObject customQObjHead = pqObj.peek();
                        while(customQObjHead.status.equals("DELIVERABLE")) {

                            //System.out.println("1.Going to remove from head: "+customQObjHead.queueMessage);
                            customQObjHead = pqObj.poll(); //Retrieving after removing the head of the queue
                            //publishProgress(customQObj.queueMessage);
                            String counterString = String.valueOf(DBcounter);
                            ContentValues databasecv = new ContentValues();
                            Log.d(TAG, "Object being pushed is: " + customQObjHead.queueMessage);
                            databasecv.put(KEY_FIELD, counterString);
                            databasecv.put(VALUE_FIELD, customQObjHead.queueMessage);
                            customQObjHead.printObject(customQObjHead); //Printing the object
                            Uri newuri = getContentResolver().insert(mUri, databasecv);
                            DBcounter++;

                            //Loop should continue only of queue is not empty
                            if(pqObj.isEmpty())
                                break;
                            customQObjHead = pqObj.peek();
                            //System.out.println("2.Found next at head: "+customQObjHead.queueMessage);
                        }
                    }

                    else if (phase == 0) {  //Element not in the queue is added to the queue

                        OutputStreamWriter streamOutput = new OutputStreamWriter(socket.getOutputStream());
                        PrintWriter serverWrites = new PrintWriter(streamOutput);
                        seqNum = seqNum + 1;  //Increment the sequnce number to order messages at the server
                        String serverSendMsg = /*msgID + "$" +*/ seqNum + "$" + globalPortNumber;
                        serverWrites.println(serverSendMsg);
                        serverWrites.flush();

                        //System.out.println("Server sends to client : "+serverSendMsg);
                        Log.d(TAG, "III Server sends to client : "+serverSendMsg);
                        serverWrites.close();
                        //seqNum = proposedPriority; //Proposed priority is replaced on seqNum

                        /*
                        //Setting the sequence number of the message at the server
                        if (seqNum > msgID)
                            proposedPriority = seqNum;
                        else
                            proposedPriority = msgID;
                        */

                        /*
                        String queueMessage;      //Message
                        int messageID;            //Message ID
                        int senderPortNumber;     //Sender AVD's port number
                        int serverSeqNum;         //Server proposes priority
                        String status;
                         */

                        customQueueObject customQObjNew = new customQueueObject();
                        customQObjNew.queueMessage = incomingMessage;
                        //customQObjNew.messageID = msgID;
                        customQObjNew.senderPortNumber = Integer.parseInt(senderProcessID);
                        customQObjNew.serverSeqNum = seqNum; //Initialized with this server's sequence number
                        customQObjNew.APServerNumber = Integer.parseInt(globalPortNumber); //Server Number that suggested the agreed priority at initialization will be -1
                        customQObjNew.status = "UNDELIVERABLE";
                        pqObj.add(customQObjNew);

                        //customQObj.printObject(customQObj); //Printing the object
                    }

                    serverReads.close();
                }
            } catch (IOException e) {
                System.out.println("I/O Exception thrown at Server!");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {

            //The following code displays what is received in doInBackground()
            String strReceived = strings[0].trim();
            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
            remoteTextView.append(strReceived + "\t\n");
            TextView localTextView = (TextView) findViewById(R.id.textView1);
            localTextView.append("\n");

            //The following code creates a file in the AVD's internal storage and stores a file.
            String filename = "SimpleMessengerOutput";
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

            //Handle failure for agreedPriority[]
            int[] serverID = new int[5]; //Holds all avd's server id to match with agreed priority
            int[] agreedPriorities = new int[5]; //Holds all avd's response for some message with proposed priority
            int maxAgreedPriority = -1, maxAgreedPriorityServerID = -1; //Decided priority after checking five agreed priorities
            int dummyPriority = 0;

            for(int i=0; i<5; i++) {
                agreedPriorities[i]=-1;
                serverID[i] = -1;
            }

            for (int i = 0; i < 5; i++) {
                try {
                    int phase=0;  //Used to differentiate first time message is sent and the second time

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORT[i]));

                    String msgToSend = msgs[0];
                    //OutputStreamWriter streamOutput = new OutputStreamWriter(socket.getOutputStream());
                    PrintWriter clientWrites = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));

                    //counter++;
                    String clientSendMsg = msgToSend.trim() + "$" + dummyPriority + "$" + globalPortNumber + "$"
                            + maxAgreedPriorityServerID + "$" + failedAVDNumber + "$"  + phase;
                    //System.out.println("Client sends message to server: " + clientSendMsg);
                    Log.i(TAG, "I Client sends message to server: " + clientSendMsg);

                    clientWrites.println(clientSendMsg);
                    clientWrites.flush();
                    //Initial message sent with some counter number set!



                    //Input for proposed priority from server
                    BufferedReader clientReads = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String proposedReply = clientReads.readLine();

                    if(proposedReply != null) {  //TODO: Handle null for BufferedReader to ignore failed AVD

                        //System.out.println("Client receives from server : " + proposedReply);
                        Log.d(TAG, "IV Client receives from server : " + proposedReply);

                        String[] proposedReplyParts = proposedReply.split("\\$");
                        //int newDummyPriority = Integer.parseInt(proposedReplyParts[0]); //Same as the variable counter
                        int probablePriority = Integer.parseInt(proposedReplyParts[0]); //Suggested priority by some server
                        int receivedFrom = Integer.parseInt(proposedReplyParts[1]); //Server's ID

                        //All avd's shall return their priority to agree upon
                        agreedPriorities[i] = probablePriority;
                        serverID[i] = receivedFrom;

                        clientReads.close();
                        clientWrites.close();
                        //Runs five times to decide upon some agreed priority
                    }

                    else //Make note of failed avd number
                        failedAVDNumber = Integer.parseInt(REMOTE_PORT[i]);

                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException" + e.toString());
                }
            }

            //Finds the greatest priority of the proposed avd's
            for(int i = 0; i < 5; i++) {
                if(Integer.parseInt(REMOTE_PORT[i]) != failedAVDNumber) {   //TODO: Ignore failed AVD
                    if (maxAgreedPriority < agreedPriorities[i]) {
                        maxAgreedPriority = agreedPriorities[i];
                        maxAgreedPriorityServerID = serverID[i];
                    }
                }
            }

            //int flag = 0;

            for (int i = 0; i < 5; i++) {
                try {
                    if (Integer.parseInt(REMOTE_PORT[i]) != failedAVDNumber) { //TODO: Ignore failed AVD and let other servers know

                        int phase = 1; //Used to differentiate first time message is sent and the second time
                        //Socket created to re-multicast agreed priority
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(REMOTE_PORT[i]));
                        PrintWriter clientWrites = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));

                        //Re-multicast message with agreed priority sent
                        String msgToSend = msgs[0];

                        /*
                        //Checks if agreed priority is in increasing order
                        if(flag == 0) {
                            System.out.println("Maximum agreed priority for " + msgToSend.trim() + " will be: " + maxAgreedPriority);
                            flag++;
                        }
                        */

                        String clientSendMsg = msgToSend.trim() + "$" + maxAgreedPriority + "$" + globalPortNumber + "$"
                                + maxAgreedPriorityServerID + "$" + failedAVDNumber + "$" + phase;

                        //System.out.println("Client sends message to server to agree: " + clientSendMsg);
                        Log.i(TAG, "V Client sends to server Agree Phase: " + clientSendMsg);
                        clientWrites.println(clientSendMsg);
                        clientWrites.flush();
                        clientWrites.close();

                    }
                } catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException" + e.toString());
                }
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
