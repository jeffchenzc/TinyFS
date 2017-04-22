package com.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.chunkserver.ChunkServer;
import com.chunkserver.Master;
import com.chunkserver.Master.ServerInfo;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	static final boolean DEBUG_SEEK = false;
	static final boolean DEBUG_DELETE = false;
	static final boolean DEBUG_SOCKET = true;
	static final int MAX_CHUNK_SIZE = 1024 * 1024;
	static final int RECORD_IS_DELETED = -2;
	
	private ServerInfo server_info;
	private Socket master_s, cs_s;
	private ObjectOutputStream master_oos, cs_oos;
	private ObjectInputStream master_ois, cs_ois;
	private DataOutputStream master_dos, cs_dos;
	private DataInputStream master_dis, cs_dis;

	private static String master_host = "localhost";
	private static int master_port = 8888;
	//private static String cs_host = "localhost";
	//private static int cs_port = 9999;
	private static int int_size = Integer.SIZE/Byte.SIZE;
	
	/**
	 * Initialize the client
	 */
	public ClientRec(){
		master_s = null;
		try {
			master_s = new Socket(master_host, master_port);
			master_oos = new ObjectOutputStream(master_s.getOutputStream());
			master_ois = new ObjectInputStream(master_s.getInputStream());
			master_dos = new DataOutputStream(master_oos);
			master_dis = new DataInputStream(master_ois);
		} catch (IOException ioe) {
			System.out.println("Connection refused: " + master_host + ":" + master_port);
			try {
				if (master_s != null) master_s.close();
			} catch (IOException ioe2) {
				System.out.println("IOE when closing connection to " + master_host + ":" + master_port);
			}
		}
	}
	
	private ServerInfo getChunkServerFromMaster(FileHandle ofh) {
		String filehandle = ofh.identifier;
		try {
			master_dos.writeChar(Master.GETSERVERFORFH);
			master_dos.writeInt(filehandle.length());
			master_dos.writeBytes(filehandle);
			master_dos.flush();
			int port = master_dis.readInt();
			String host = readStringFromMaster();
			ServerInfo server_info = new ServerInfo(host, port);
			return server_info;
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}
	
	private void connectToServer(String cs_host, int cs_port) {
		cs_s = null;
		try {
			cs_s = new Socket(cs_host, cs_port);
			if (cs_oos == null ) cs_oos = new ObjectOutputStream(cs_s.getOutputStream());
			if (cs_ois == null ) cs_ois = new ObjectInputStream(cs_s.getInputStream());
			if (cs_dos == null ) cs_dos = new DataOutputStream(cs_oos);
			if (cs_dis == null ) cs_dis = new DataInputStream(cs_ois);
		} catch (IOException ioe) {
			try {
				if (cs_s != null) cs_s.close();
			} catch (IOException ioe2) {
				System.out.println("IOE when closing connection to " + cs_host + ":" + cs_port);
			}
		}
	}
	
	private RID readRID() {
		RID rid = new RID();
		try{
			rid.chunkName = readStringFromServer();
			rid.offst = cs_dis.readInt();
			rid.pointer_index = cs_dis.readInt();
			rid.size = cs_dis.readInt();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return rid;
	}
	
	private TinyRec readRec() {
		TinyRec rec = new TinyRec();
		RID rid = readRID();
		byte[] payload = new byte[0];
		try {
			int p_length = cs_dis.readInt();
			cs_dis.read(payload, 0, p_length);
			rec.setRID(rid);
			rec.setPayload(payload);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return rec;
	}
	
	public void writeRID(RID rid) {
		try{
			writeStringToServer(rid.chunkName);
			cs_dos.writeInt(rid.offst);
			cs_dos.writeInt(rid.pointer_index);
			cs_dos.writeInt(rid.size);
			cs_dos.flush();
			cs_oos.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/**
	 * @param ofh
	 * gets the server info (host,port) from the master
	 * connects to the server (sets the cs_dos, cs_dis, etc)
	 */
	private void getServerInfo(FileHandle ofh){
		if ( server_info == null ) { 
			server_info = getChunkServerFromMaster(ofh);
			connectToServer(server_info.host, server_info.port);
		}
	}
	
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		FSReturnVals v;
		getServerInfo(ofh);
		try {
			cs_dos.writeChar(ChunkServer.APPENDRECORD);
			cs_dos.flush();
			writeStringToServer(ofh);
			cs_dos.writeInt(payload.length);
			cs_dos.write(payload, 0, payload.length);
			writeRID(RecordID);
			v = FSReturnVals.valueOf(readStringFromServer());
			return v;
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return v.Fail;
	}
	
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID, int counter) {
		FSReturnVals v = AppendRecord(ofh, payload, RecordID);
		if (DEBUG_DELETE) System.out.println(">\tAPPENDRECORD " + counter + " " + v.toString());
		return v;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		FSReturnVals v;
		getServerInfo(ofh);
		try{
			cs_dos.writeChar(ChunkServer.DELETERECORD);
			cs_dos.flush();
			writeStringToServer(ofh);
			writeRID(RecordID);
			v = FSReturnVals.valueOf(readStringFromServer());
			return v;
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		FSReturnVals v;
		getServerInfo(ofh);
		writeStringToServer(ofh);
		rec = readRec();
		v = FSReturnVals.valueOf(readStringFromServer());
		return v;
	}
	
	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		FSReturnVals v;
		getServerInfo(ofh);
		writeStringToServer(ofh);
		rec = readRec();
		v = FSReturnVals.valueOf(readStringFromServer());
		return v;
	}
	
	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		FSReturnVals v;
		getServerInfo(ofh);
		writeStringToServer(ofh);
		writeRID(pivot);
		rec = readRec();
		v = FSReturnVals.valueOf(readStringFromServer());
		return v;
	}
	
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec, int counter) {
		System.out.print(">\t\t" + counter);
		return ReadNextRecord(ofh, pivot, rec);
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		FSReturnVals v;
		getServerInfo(ofh);
		writeStringToServer(ofh);
		writeRID(pivot);
		rec = readRec();
		v = FSReturnVals.valueOf(readStringFromServer());
		return v;
	}
	
	/**
	 * @return String that was read in from Master
	 * reads an int [length], then reads [length] number of bytes from Master's socket
	 */
	private String readStringFromMaster(){
		int length;
		byte[] str;
		String ans = "";
		try {
			length = master_dis.readInt(); //length of payload
			str = new byte[length];
			for (int i = 0; i < length; i++) {
				str[i] = master_dis.readByte();
			}
			ans = new String(str, Charset.forName("UTF-8"));
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
		return ans;
	}
	
	private void writeStringToMaster(String payload) {
		int payload_length;
		byte[] payload_bytes = payload.getBytes(Charset.forName("UTF-8"));
		
		payload_length = payload_bytes.length;
		
		try{
			master_dos.writeInt(payload_length);
			master_dos.write(payload_bytes, 0, payload_length);
			master_dos.flush();
			master_oos.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private String readStringFromServer(){
		int length;
		byte[] str;
		String ans = "";
		try {
			length = cs_dis.readInt(); //length of payload
			str = new byte[length];
			for (int i = 0; i < length; i++) {
				str[i] = cs_dis.readByte();
			}
			ans = new String(str, Charset.forName("UTF-8"));
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
		return ans;
	}
	
	private void writeStringToServer(String payload) {
		int payload_length;
		byte[] payload_bytes = payload.getBytes(Charset.forName("UTF-8"));
		
		payload_length = payload_bytes.length;
		
		try{
			cs_dos.writeInt(payload_length);
			cs_dos.write(payload_bytes, 0, payload_length);
			cs_dos.flush();
			cs_oos.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

}
