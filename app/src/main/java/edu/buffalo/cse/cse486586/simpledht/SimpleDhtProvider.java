package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private String myPortHash;
    private static String currentPort;

    static final String[] ports={"11108", "11112", "11116", "11120", "11124"};
    protected static ArrayList<String> REMOTE_PORTS = new ArrayList<String>(Arrays.asList(ports));
    static final int SERVER_PORT = 10000;
    protected final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    final static int nodeJoin = 0;
    final static int notify = 1;
    final static int lookUpOperation = 2;
    final static int insertOperation = 3;
    final static int queryOperation = 4;
    final static int deleteOperation = 5;
    final static int queryFromAllOperation = 6;
    final static int deleteFromAllOperation = 7;

    private DBHelper dbHelper;
    private static final String DBNAME = "SimpleDhtDB";
    private SQLiteDatabase database;

    protected static final String KEY="key";
    protected static final String VALUE="value";

    static Map<String,String> hashToPort = new HashMap<String,String>();

    static List<String> activePorts = new ArrayList<String>();
    static List<String> dhtList = new ArrayList<String>();

    static String predecessor;
    static String successor;

    static int isUpdated=1;

    private static String maxHash;
    private static String minHash;


    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

         database = dbHelper.getWritableDatabase();
        int rowsDeleted=0;
        try {
            Log.e("My code", "Inside the try block for delete");
            String hashElement = genHash(selection);
            String[] args = new String[1];
            args[0] = selection;

            if(selection.equals("@")){
                rowsDeleted = database.delete(DBNAME, null, args);

            } else if (selection.equals("*")) {
                if (predecessor != null) {
                    rowsDeleted = database.delete(DBNAME, null, args);

                    int rowsDeletedFromDHT = deleteFromAll(successor,"*",currentPort);
                    rowsDeleted+=rowsDeletedFromDHT;

                }
                else{
                    rowsDeleted = database.delete(DBNAME, null, args);
                }
            }
            else{
            if (predecessor != null) {
                int prevEmulator = Integer.parseInt(predecessor) / 2;
                String predecessorHash = genHash(String.valueOf(prevEmulator));

                if (hashElement.compareTo(maxHash) > 0 || hashElement.compareTo(minHash) < 0) {
                    if (!myPortHash.equals(minHash)) {
                        rowsDeleted = deleteFrom(hashToPort.get(minHash), selection);
                    } else {
                        rowsDeleted = database.delete(DBNAME, "key =?", args);
                    }

                } else {
                    if (hashElement.compareTo(predecessorHash) > 0 && hashElement.compareTo(myPortHash) <= 0) {
                        rowsDeleted = database.delete(DBNAME, "key =?", args);
                    } else {
                        String id = lookup(successor, hashElement);
                        rowsDeleted = deleteFrom(id, selection);
                    }
                }
            } else {
                rowsDeleted = database.delete(DBNAME, "key =?", args);
            }
        }


        }catch (Exception e){
            Log.e("My code", "SQL delete failed due to " + e);
            e.printStackTrace();
        }

        Log.e("My code", "Number of rows deleted " + rowsDeleted);
        return rowsDeleted;
    }

    private int deleteFrom(String id, String selection) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        int returnRows=0;

        //Log.e("My code", "deleteFrom at : "+currentPort);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(deleteOperation));
            bw.newLine();
            bw.flush();

            bw.write(selection);
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            returnRows=Integer.parseInt(br.readLine());

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My code", "End of deleteFrom at : "+currentPort);
        return returnRows;
    }

    private int deleteFromAll(String id, String selection, String originPort) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        int delCount=0;

        Log.e("My code", "Start of deleteFromAll at : "+currentPort+" with destination port : "+id);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(deleteFromAllOperation));
            bw.newLine();
            bw.flush();

            bw.write("@");
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            delCount = Integer.parseInt(br.readLine());
            Log.e("My code", "deleteFromAll Received delCount : "+delCount);

            String nextSuccessor = br.readLine();
            Log.e("My code", "deleteFromAll Received Successor : "+nextSuccessor);

            int recCount=0;

            if(!nextSuccessor.equals(originPort)){
                recCount= deleteFromAll(nextSuccessor, selection, originPort);
            }

            if(recCount!=0){
                delCount = delCount + recCount;
            }

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.e("My code", "End of deleteFromAll at : "+currentPort+" with destination port : "+id);
        return delCount;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized Uri insert(Uri uri, ContentValues values)  {

        // TODO Auto-generated method stub
        //Log.e("My code", " Getting a writable instance of a db");
        database = dbHelper.getWritableDatabase();

        long rowId=0;
        try {
            //Log.e("My code", " Inside the try block for insert");

            String hashKey = genHash(values.getAsString(KEY));
/*            Log.e("My code", " Hashkey : "+hashKey);
        Log.e("My code", " Predecessor hash : "+predecessor);
        Log.e("My code", " My port hash : "+myPortHash);
        Log.e("My code", " Max hash : "+maxHash);
        Log.e("My code", " Min hash : "+minHash);*/
            if(predecessor!=null) {
                int prevEmulator = Integer.parseInt(predecessor) / 2;
                String predecessorHash = genHash(String.valueOf(prevEmulator));
                if (hashKey.compareTo(maxHash) > 0 || hashKey.compareTo(minHash) < 0) {
                    if (!myPortHash.equals(minHash)) {
                        Uri retUri = insertAt(hashToPort.get(minHash), values);
                        return retUri;
                    } else {
                        rowId = database.replace(DBNAME, "", values);
                        Log.e("My code", "Insertion successful in db at port number is : " + currentPort);
                    }

                } else {
                    if (hashKey.compareTo(predecessorHash) > 0 && hashKey.compareTo(myPortHash) <= 0) {
                        rowId = database.replace(DBNAME, "", values);
                        Log.e("My code", "Insertion successful in db at port number is : " + currentPort);
                    } else {
                        String id = lookup(successor, hashKey);
                        Uri retUri = insertAt(id, values);
                        return retUri;
                    }

                }
            }
            else{
                rowId = database.replace(DBNAME, "", values);
            }

        } catch (Exception e) {
            Log.e("My code", "SQL write failed due to " + e);
            e.printStackTrace();
        }

        if (rowId > 0) {
            //Log.e("My code", "Insertion successful. Row "+rowId+" added to the table");
            getContext().getContentResolver().notifyChange(uri, null);
        }


        Log.v("insert", values.toString());
        return uri;
    }

    private Uri insertAt(String id, ContentValues values) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        Uri retUri=null;

        //Log.e("My code", "insertAt at : "+currentPort);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(insertOperation));
            bw.newLine();
            bw.flush();

            bw.write(String.valueOf(values.get(KEY)));
            bw.newLine();
            bw.flush();

            bw.write(String.valueOf(values.get(VALUE)));
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            retUri = Uri.parse(br.readLine());

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My code", "End of insertAt at : "+currentPort);
        return retUri;
    }

    private String lookup(String destination, String hashKey) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        String retDest=null;

        Log.e("My code", "Lookup at : "+currentPort+" on : "+destination);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destination));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(lookUpOperation));
            bw.newLine();
            bw.flush();


            bw.write(String.valueOf(hashKey));
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            retDest = br.readLine();

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.e("My code", "End of lookup at : "+currentPort+" with value : "+retDest);
        return retDest;

    }

    private void establishDHT() {
        Log.e("My code", "Calling establishDHT");
        dhtList= new ArrayList<String>();
        if(!currentPort.equals("11108")){
            Log.e("My code", "establishDHT called from wrong process");
            return;
        }
        for(int i=0;i<activePorts.size();i++){
            try {
                dhtList.add(genHash(String.valueOf(Integer.parseInt(activePorts.get(i))/2)));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(dhtList);

        maxHash=dhtList.get(dhtList.size()-1);
        minHash=dhtList.get(0);

        Log.e("My code", "DHT Established with maxHash - "+maxHash+", i.e. port - "+hashToPort.get(maxHash)+" minHash - "+minHash+" i.e. port - "+hashToPort.get(minHash));
        for (int i=0;i<dhtList.size();i++){
            Log.e("My code", "Dht List element "+i+" : "+dhtList.get(i));
        }
    }

    public String getSuccessor(String remotePortNo){
        //Log.e("My code", "Calling getSuccessor");

        if(!currentPort.equals("11108")){
            Log.e("My code", "getSuccessor called from wrong process");
            return null;
        }

        int emulatorNo = Integer.parseInt(remotePortNo)/2;
        try {
            String remoteHash = genHash(String.valueOf(emulatorNo));
            for(int i=0;i<dhtList.size();i++){
                //Log.e("My code", " i = "+i+", current dhtElement : "+hashToPort.get(dhtList.get(i)));

                if(dhtList.get(i).equals(remoteHash)){
                    if(i!=dhtList.size()-1){
                        String ret=hashToPort.get(dhtList.get(i+1));
                        //Log.e("My code", "Returning successor : " + ret);
                        return ret;
                    }
                    else if(i==dhtList.size()-1){
                        String ret=hashToPort.get(dhtList.get(0));
                        //Log.e("My code", "Returning successor : "+ret);
                        return ret;
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("My code", "Given process not in dht");

        return "11108";
    }

    public String getPredecessor(String remotePortNo){
        //Log.e("My code", "Calling getPredecessor");

        if(!currentPort.equals("11108")){
            Log.e("My code", "getPredecessor called from wrong process");
            return null;
        }

        int emulatorNo = Integer.parseInt(remotePortNo)/2;
        try {
            String remoteHash = genHash(String.valueOf(emulatorNo));
            for(int i=0;i<dhtList.size();i++){
                if(dhtList.get(i).equals(remoteHash)){
                    if(i!=0){
                        String ret=hashToPort.get(dhtList.get(i-1));
                        //Log.e("My code", "Returning predecessor : " + ret);
                        return ret;
                    }
                    else if(i==0){
                        String ret=hashToPort.get(dhtList.get(dhtList.size()-1));
                        //Log.e("My code", "Returning predecessor : "+ret);
                        return ret;
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        Log.e("My code", "Given process not in dht");

        return "11108";
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        Context context = getContext();
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        currentPort=myPort;
        Log.e("My code", "My Port number : " + myPort);
        dbHelper = new DBHelper(context,DBNAME,null,1);
        try {
            int emulatorNo=(Integer.parseInt(myPort))/2;
            Log.e("My code", "My Emulator number : " + emulatorNo);
            myPortHash=(genHash(String.valueOf(emulatorNo)));
            //Log.e("My code", "Hash value of my port string : " + getMyPortHash());

            for(int i=0;i<ports.length;i++){
                emulatorNo=(Integer.parseInt(ports[i]))/2;
                hashToPort.put(genHash(String.valueOf(emulatorNo)),ports[i]);
                //Log.e("My code", "Adding port : " + ports[i] + " to the hashmap with hash :" + genHash(ports[i]));
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            //Log.e("My code", "Creating Server Thread");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            if(!myPort.equals("11108")){
                new NodeJoin().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
                activePorts.add(myPort);
            }
            else{
                activePorts.add(myPort);
                establishDHT();
                successor=getSuccessor("11108");
                predecessor=getPredecessor("11108");
            }

        }catch(UnknownHostException e) {
            Log.e("My code", "Can't create a client socket due to exception : " + e);
            return false;

        } catch(IOException e) {
            Log.e("My code", "Can't create a ServerSocket due to exception : " + e);
            return false;
        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        database = dbHelper.getReadableDatabase();

        Cursor c1=null;

        try{

            String hashElement = genHash(selection);
            String[] args = new String[1];
            args[0] = selection;

            if(selection.equals("@")){
                Log.e("My code", "Querying for @ ");
                c1 = database.query(
                        DBNAME,  // The table to query
                        null,                               // The columns to return
                        null,                                    // The columns for the WHERE clause
                        null,                                       // The values for the WHERE clause
                        null,                                     // don't group the rows
                        null,                                     // don't filter by row groups
                        null                                 // The sort order
                );
            }
            else if(selection.equals("*")){
                Log.e("My code", "Querying for * ");
                if(predecessor!=null){
                    Cursor c0=database.query(
                            DBNAME,  // The table to query
                            null,                               // The columns to return
                            null,                                    // The columns for the WHERE clause
                            null,                                       // The values for the WHERE clause
                            null,                                     // don't group the rows
                            null,                                     // don't filter by row groups
                            null                                 // The sort order
                    );


                    Cursor c2=queryFromAll(successor,"*",currentPort);

                    c1 = new MergeCursor(new Cursor[]{c0,c2});
                }
                else{
                    c1=database.query(
                            DBNAME,  // The table to query
                            null,                               // The columns to return
                            null,                                    // The columns for the WHERE clause
                            null,                                       // The values for the WHERE clause
                            null,                                     // don't group the rows
                            null,                                     // don't filter by row groups
                            null                                 // The sort order
                    );
                }

            }
            else{
                if(predecessor!=null){
                    int prevEmulator = Integer.parseInt(predecessor)/2;
                    String predecessorHash = genHash(String.valueOf(prevEmulator));

                    if (hashElement.compareTo(maxHash) > 0 || hashElement.compareTo(minHash) < 0) {
                        if (!myPortHash.equals(minHash)) {
                            c1 = queryFrom(hashToPort.get(minHash), selection);
                        } else {
                            c1 = database.query(
                                    DBNAME,  // The table to query
                                    projection,                               // The columns to return
                                    "key =?",                                    // The columns for the WHERE clause
                                    args,                                       // The values for the WHERE clause
                                    null,                                     // don't group the rows
                                    null,                                     // don't filter by row groups
                                    null                                 // The sort order
                            );
                        }

                    } else {
                        if (hashElement.compareTo(predecessorHash) > 0 && hashElement.compareTo(myPortHash) <= 0) {

                            c1 = database.query(
                                    DBNAME,  // The table to query
                                    projection,                               // The columns to return
                                    "key =?",                                    // The columns for the WHERE clause
                                    args,                                       // The values for the WHERE clause
                                    null,                                     // don't group the rows
                                    null,                                     // don't filter by row groups
                                    null                                 // The sort order
                            );
                        } else {
                            String id = lookup(successor, hashElement);
                            c1  = queryFrom(id, selection);
                        }
                    }
                }
                else{
                    c1 = database.query(
                            DBNAME,  // The table to query
                            projection,                               // The columns to return
                            "key =?",                                    // The columns for the WHERE clause
                            args,                                       // The values for the WHERE clause
                            null,                                     // don't group the rows
                            null,                                     // don't filter by row groups
                            null                                 // The sort order
                    );
                }
            }



            if (c1.moveToFirst())
                Log.e("My code", "The column contents for key : " + c1.getString(0) + " value : " + c1.getString(1));
        } catch (Exception e) {
            Log.e("My code", "SQL query failed due to " + e);
            e.printStackTrace();
        }

        Log.v("query", selection);
        return c1;
    }

    private Cursor queryFrom(String id, String selection) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        MatrixCursor matrixCursor=null;

        //Log.e("My code", "insertAt at : "+currentPort);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(queryOperation));
            bw.newLine();
            bw.flush();

            bw.write(selection);
            bw.newLine();
            bw.flush();


            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String receivedKey=br.readLine();
            String receivedValue=br.readLine();

            String[] columns = new String[] { "key", "value"};
            matrixCursor= new MatrixCursor(columns);
            matrixCursor.addRow(new String[] { receivedKey, receivedValue });

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //Log.e("My code", "End of insertAt at : "+currentPort);
        return matrixCursor;
    }

    private Cursor queryFromAll(String id, String selection, String originPort) {
        BufferedWriter bw=null;
        Socket clientSocket=null;
        Cursor matrixCursor=null;

        Log.e("My code", "Start of queryFromAll at : "+currentPort+" with destination port : "+id);
        try {
            clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(id));

            OutputStream os = clientSocket.getOutputStream();
            bw = new BufferedWriter(new OutputStreamWriter(os));

            bw.write(String.valueOf(queryFromAllOperation));
            bw.newLine();
            bw.flush();

            bw.write("@");
            bw.newLine();
            bw.flush();

            InputStream is = clientSocket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            int count = Integer.parseInt(br.readLine());
            Log.e("My code", "queryFromAll Received Count : "+count);

            String[] columns = new String[] { "key", "value"};
            matrixCursor= new MatrixCursor(columns);
            MatrixCursor tempMatrixCursor = new MatrixCursor(columns);
            Cursor receivedCursor = null;

            for(int k=0;k<count;k++){
                String receivedKey=br.readLine();
                String receivedValue=br.readLine();
                tempMatrixCursor.addRow(new String[] { receivedKey, receivedValue });
            }

            String nextSuccessor = br.readLine();
            Log.e("My code", "queryFromAll Received Successor : "+nextSuccessor);

            if(!nextSuccessor.equals(originPort)){
                receivedCursor = queryFromAll(nextSuccessor,selection,originPort);
            }

            if(receivedCursor!=null){
                matrixCursor = new MergeCursor(new Cursor[]{tempMatrixCursor,receivedCursor});
            }
            else{
                matrixCursor = tempMatrixCursor;
            }

            bw.write("end");
            bw.newLine();
            bw.flush();

        } catch (UnknownHostException e) {
            Log.e("My code", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("My code", "ClientTask socket IOException on port " + currentPort);
        } catch (Exception e) {
            Log.e("My code", "Exception : "+e);
        }finally {
            if(clientSocket!=null){
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Log.e("My code", "End of queryFromAll at : "+currentPort+" with destination port : "+id);
        return matrixCursor;
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class NodeJoin extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {


            BufferedWriter bw=null;
            Socket clientSocket=null;
            String myPortNo = msgs[0];
            Log.e("My code", "Start of node join in client : "+myPortNo);
            try {
                Log.e("My code", "Node Join - Attempting to create client socket to port 11108");
                clientSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);

                activePorts.add(String.valueOf(11108));

                OutputStream os = clientSocket.getOutputStream();
                bw = new BufferedWriter(new OutputStreamWriter(os));

                bw.write(String.valueOf(nodeJoin));
                bw.newLine();
                bw.flush();


                bw.write(String.valueOf(myPortNo));
                bw.newLine();
                bw.flush();

                InputStream is = clientSocket.getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                br.readLine();

            } catch (UnknownHostException e) {
                Log.e("My code", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("My code", "ClientTask socket IOException on port " + Integer.parseInt(myPortNo)*2);
            } catch (Exception e) {
                Log.e("My code", "Exception : "+e);
            }finally {
                if(clientSocket!=null){
                    try {
                        clientSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            Log.e("My code", "End of node join in client : "+myPortNo);
            return null;
        }
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            BufferedReader bReader=null;
            Socket srvSocket = null;

            int messageCounter = 0;
            while (true) {
                try {
                    //Log.e("My code", "Try block in Server thread");
                    srvSocket = serverSocket.accept();
                    Log.e("My code", "Accepted the socket");

                    InputStream iStream = srvSocket.getInputStream();
                    bReader = new BufferedReader(new InputStreamReader(iStream));

                    int operation = Integer.parseInt(bReader.readLine());
                    Log.e("My code", "The operation to perform is : "+operation);

                    if(operation==nodeJoin){
                        Log.e("My code", "Start of node join in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String joinPort = bReader.readLine();
                        activePorts.add(joinPort);
                        isUpdated=0;
                        Log.e("My code", "Added "+joinPort+" to active ports");

                        bWriter.write("end");
                        bWriter.newLine();
                        bWriter.flush();

                        Log.e("My code", "End of node join in server");

                    }
                    if(isUpdated==0){
                        isUpdated=1;
                        establishDHT();
                        updateDHT();
                    }

                    if(operation==notify){
                        //Log.e("My code", "Start of notify in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        successor=bReader.readLine();
                        Log.e("My code", "Updated successor of "+currentPort+" is : "+successor);

                        predecessor=bReader.readLine();
                        Log.e("My code", "Updated predecessor of "+currentPort+" is : "+predecessor);

                        maxHash=bReader.readLine();
                        minHash=bReader.readLine();

                        bWriter.write("end");
                        bWriter.newLine();
                        bWriter.flush();

                        //Log.e("My code", "End of notify in server");

                    }

                    if(operation==lookUpOperation){
                        String receivedKeyHash = bReader.readLine();

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        int prevEmulator = Integer.parseInt(predecessor)/2;
                        if(receivedKeyHash.compareTo(genHash(String.valueOf(prevEmulator)))>0 && receivedKeyHash.compareTo(myPortHash)<=0){
                            bWriter.write(currentPort);
                            bWriter.newLine();
                            bWriter.flush();
                        }
                        else {
                            bWriter.write(lookup(successor, receivedKeyHash));
                            bWriter.newLine();
                            bWriter.flush();
                        }
                        bReader.readLine();

                    }

                  else if(operation==insertOperation){
                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        String receivedValue = bReader.readLine();

                        ContentValues cv= new ContentValues();
                        cv.put(KEY,receivedKey);
                        cv.put(VALUE,receivedValue);

                        Uri uriToReturn = insert(uri,cv);

                        bWriter.write(uriToReturn.toString());
                        bWriter.newLine();
                        bWriter.flush();

                        bReader.readLine();
                    }

                    else if(operation == queryOperation){
                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        Cursor cursor = query(uri,null,receivedKey,null,null,null);

                        bWriter.write(cursor.getString(0));
                        bWriter.newLine();
                        bWriter.flush();

                        bWriter.write(cursor.getString(1));
                        bWriter.newLine();
                        bWriter.flush();

                        bReader.readLine();

                    }

                    else if(operation==deleteOperation){
                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();
                        int rows = delete(uri,receivedKey,null);

                        bWriter.write(String.valueOf(rows));
                        bWriter.newLine();
                        bWriter.flush();

                        bReader.readLine();
                    }
                    else if(operation == queryFromAllOperation){
                        Log.e("My code", "Start of queryFromAll in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();

                        Cursor cursor = query(uri,null,receivedKey,null,null,null);

                        bWriter.write(String.valueOf(cursor.getCount()));
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My code", "queryFromAll sent Count : "+cursor.getCount());

                        for(int l=0;l<cursor.getCount();l++){
                            bWriter.write(cursor.getString(0));
                            bWriter.newLine();
                            bWriter.flush();

                            bWriter.write(cursor.getString(1));
                            bWriter.newLine();
                            bWriter.flush();

                            cursor.moveToNext();

                        }

                        bWriter.write(successor);
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My code", "queryFromAll sent Successor : " + successor);


                        bReader.readLine();
                        Log.e("My code", "End of queryFromAll in server. Successor : " + successor);
                    }

                    else if(operation == deleteFromAllOperation){
                        Log.e("My code", "Start of deleteFromAll in server");

                        OutputStream oStream = srvSocket.getOutputStream();
                        BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(oStream));

                        String receivedKey = bReader.readLine();

                        int rows = delete(uri, receivedKey, null);

                        bWriter.write(String.valueOf(rows));
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My code", "deleteFromAll sent delCount : "+rows);

                        bWriter.write(successor);
                        bWriter.newLine();
                        bWriter.flush();
                        Log.e("My code", "deleteFromAll sent Successor : " + successor);


                        bReader.readLine();
                        Log.e("My code", "End of queryFromAll in server. Successor : " + successor);
                    }

                } catch (IOException e) {
                    Log.e("My code - Server", "Failed to open Server Port due to : " + e);
                    e.printStackTrace();
                    break;
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        srvSocket.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return null;
        }
    }

    protected void updateDHT() {

            BufferedWriter bw=null;
            Socket clientSocketArray[]=new Socket[activePorts.size()];
            Log.e("My code", "Start of updateDHT");

            for(int i=0;i<activePorts.size();i++){

                if(activePorts.get(i).equals(currentPort)) {
                    successor=getSuccessor(currentPort);
                    Log.e("My code", "Updated successor of "+currentPort+" is : "+successor);

                    predecessor=getPredecessor(currentPort);
                    Log.e("My code", "Updated predecessor of "+currentPort+" is : "+predecessor);

                    continue;
                }
            try {
                Log.e("My code", "updateDHT to port : " + activePorts.get(i));
                clientSocketArray[i] = new Socket();
                clientSocketArray[i].connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(activePorts.get(i))));

                OutputStream os = clientSocketArray[i].getOutputStream();
                bw = new BufferedWriter(new OutputStreamWriter(os));

                bw.write(String.valueOf(notify));
                bw.newLine();
                bw.flush();

                bw.write(getSuccessor(activePorts.get(i)));
                bw.newLine();
                bw.flush();

                bw.write(getPredecessor(activePorts.get(i)));
                bw.newLine();
                bw.flush();

                bw.write(maxHash);
                bw.newLine();
                bw.flush();

                bw.write(minHash);
                bw.newLine();
                bw.flush();

                InputStream is = clientSocketArray[i].getInputStream();
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                br.readLine();

            } catch (UnknownHostException e) {
                Log.e("My code", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("My code", "ClientTask socket IOException on port " + currentPort);
            } catch (Exception e) {
                Log.e("My code", "Exception : "+e);
            }finally {
                if(clientSocketArray[i]!=null){
                    try {
                        clientSocketArray[i].close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            Log.e("My code", "End of updateDHT for : "+activePorts.get(i));
        }
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

    private class DBHelper extends SQLiteOpenHelper {

        private static final String SQL_TABLE = "CREATE TABLE " +
                "SimpleDhtDB" +                       // Table's name
                "(" +                           // The columns in the table
                " key TEXT PRIMARY KEY, " +
                " value TEXT )";

        private static final String SQL_DELETE_ENTRIES =
                "DROP TABLE IF EXISTS SimpleDhtDB";

        public DBHelper(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {
            super(context, name, factory, version);
        }

        @Override
        public void onCreate(SQLiteDatabase db) {
            db.execSQL(SQL_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
    }

}