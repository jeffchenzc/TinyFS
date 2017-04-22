package com.chunkserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
//import java.util.Arrays;
import java.util.LinkedList;
import java.util.Vector;

import com.client.Client;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;
import com.client.ClientFS.FSReturnVals;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

//Woonghee's Data is pushed!

public class ChunkServer {
	static final boolean DEBUG_SEEK = false;
	static final boolean DEBUG_DELETE = false;
	static final boolean DEBUG_SOCKET = true;
	static final boolean DEBUG_SERVER = true;
	static final int MAX_CHUNK_SIZE = 1024 * 1024;
	static final int RECORD_IS_DELETED = -2;
	
	public static final char APPENDRECORD		= '1';
	public static final char DELETERECORD		= '2';
	public static final char READFIRSTRECORD	= '3';
	public static final char READLASTRECORD 	= '4';
	public static final char READNEXTRECORD 	= '5';
	public static final char READPREVRECORD 	= '6';
	
	private Socket s;
	private ServerSocket ss;
	
	private Vector<ChunkServerThread> threads;
	private static String host = "localhost";
	private static int port = 9999;
	private static int int_size = Integer.SIZE/Byte.SIZE;
	
	public ChunkServer(){
		
		//start serversocket
		threads = new Vector<ChunkServerThread>();
		ss = null;
		try {
			ss = new ServerSocket(port);
			if (DEBUG_SERVER) System.out.println("Successfully started server on localhost:" + port);
			while (true) {
				if (DEBUG_SERVER) System.out.println("Waiting for connections...");
				s = ss.accept();
				if (DEBUG_SERVER) System.out.println("Connection from " + s.getInetAddress());
				ChunkServerThread cst = new ChunkServerThread(s, this);
				threads.add(cst);
			}
		} catch (BindException be) {
			if (DEBUG_SERVER) System.out.println("A server is already running on this port");
		} catch (IOException ioe) {
			if (DEBUG_SERVER) System.out.println("Exception while accepting connections on server");
			ioe.printStackTrace();
		} finally {
			if (ss != null) {
				try { ss.close(); }
				catch (IOException ioe) { if (DEBUG_SERVER)System.out.println("Exception while closing server"); }
			}
		}
	}

	private FSReturnVals AppendRecordToNewChunk(FileHandle ofh, byte[] payload, RID RecordID) {
		if (DEBUG_SEEK) System.out.println(">\t Append to new chunk");
		RandomAccessFile raf = null;
		String new_chunk_name = ofh.fileName + "_chunk_" + ofh.chunks.size();
		ofh.chunks.add(new_chunk_name);
		try {
			raf = new RandomAccessFile(new_chunk_name,"rw");
			raf.setLength((long) MAX_CHUNK_SIZE);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		if(payload.length > MAX_CHUNK_SIZE - 8){
			System.out.print("APPENDTONEWCHUNK recordtoolong");
			return FSReturnVals.RecordTooLong;
		}
		byte[] counter = ByteBuffer.allocate(4).putInt(1).array();
		byte[] pointer = ByteBuffer.allocate(4).putInt(4+payload.length).array();
		try {
			raf.seek(0);
			raf.write(counter, 0, 4);
			raf.seek(4);
			raf.write(payload, 0, payload.length);
			raf.seek(MAX_CHUNK_SIZE-4);
			raf.write(pointer, 0, 4);
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("APPENDTONEWCHUNK success");
		return FSReturnVals.Success;
	}
		
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID, int counter) {
		FSReturnVals v = AppendRecord(ofh, payload, RecordID);
		if (DEBUG_DELETE) System.out.println(">\tAPPENDRECORD " + counter + " " + v.toString());
		return v;
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
		RandomAccessFile raf = null;
		if(ofh.chunks.size() == 0){
			FSReturnVals v = AppendRecordToNewChunk(ofh, payload, RecordID);
			if (v == FSReturnVals.RecordTooLong) return v;
			return FSReturnVals.ReadToNextChunk;
		}
		String chunk = ofh.chunks.getLast();
		int max_pointer = max_pointer(chunk);
		if (DEBUG_SEEK) System.out.println(">\t max pointers: " + max_pointer);
		byte[] last_offset = new byte[4];
		try {
			raf = new RandomAccessFile(ofh.chunks.getLast(),"rw");
			raf.seek(MAX_CHUNK_SIZE - 4*(max_pointer));
			raf.read(last_offset, 0, 4);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException ioe){
			ioe.printStackTrace();
		}
		
		int last_offset_int = ByteBuffer.wrap(last_offset).getInt();
		if(payload.length > MAX_CHUNK_SIZE - last_offset_int - 4*(max_pointer+1)){
			FSReturnVals v = AppendRecordToNewChunk(ofh, payload, RecordID);
			if (v == FSReturnVals.RecordTooLong) return v;
			return FSReturnVals.ReadToNextChunk;
		}
		byte[] number = ByteBuffer.allocate(4).putInt(max_pointer + 1).array();
		byte[] pointer = ByteBuffer.allocate(4).putInt(payload.length+last_offset_int).array();
		try {
			if (DEBUG_SEEK) System.out.println(">\tincrement max pointer: " + (max_pointer+1));
			raf.seek(0);
			raf.write(number, 0, 4);
			raf.seek(last_offset_int);
			raf.write(payload, 0, payload.length);
			raf.seek(MAX_CHUNK_SIZE-4*(max_pointer + 1));
			raf.write(pointer, 0, 4);
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return FSReturnVals.Success;
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
		if (DEBUG_DELETE) System.out.println(">\tDELETERECORD: " + RecordID.chunkName + " " + RecordID.pointer_index);
		FSReturnVals v;
		int offst = RecordID.offst;
		String chunk = RecordID.chunkName;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(chunk, "rw");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int max_pointer = max_pointer(chunk);
		int current_offset = -1;
		int current_index = MAX_CHUNK_SIZE;
		for(int i = 0; i < max_pointer; i++){
			current_offset = getNextPointer(current_index, chunk, max_pointer, true);
			if(current_offset == -1){
				v = FSReturnVals.RecDoesNotExist;
				if (DEBUG_DELETE) System.out.println(">\tDELETERECORD: " + v.toString());
				return v;
			} else if (current_offset < 0){
				//current offset is negative, this place's data was freed
				if (current_offset == (-1 * offst)) {
					v = FSReturnVals.RecDoesNotExist;
					if (DEBUG_DELETE) System.out.println(">\tDELETERECORD: " + v.toString());
					return v;
				}
			} else if (current_offset == offst){
				int new_offset = current_offset * -1;
				byte[] write = ByteBuffer.allocate(4).putInt(new_offset).array();
				try {
					current_index -= 4;
					raf.seek(current_index);
					raf.write(write, 0, 4);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return FSReturnVals.Success;
			}
			
			current_index -= 4;
		}
		v = FSReturnVals.Fail;
		if (DEBUG_DELETE) System.out.println(">\tDELETERECORD: " + v.toString());
		return FSReturnVals.Fail;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		String first_chunk_name = ofh.chunks.getFirst();
		int current_index = MAX_CHUNK_SIZE;
		
		System.out.println(">\tREADFIRSTRECORD: " + first_chunk_name + " " + current_index);
		int status = readChunkData(first_chunk_name, current_index, rec, true, ofh);
		//for (int i = 0; i < ofh.chunks.size(); i++) {
			//if (status == -1){ status = readChunkData(ofh.chunks.get(i), current_index, rec, true); }
		//}

		return FSReturnVals.BadRecID; //no valid pointers found.
	}
	
	private int max_pointer(String chunk){
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(chunk, "r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		byte[] counter = new byte[4];
		try {
			raf.seek(0);
			raf.read(counter, 0, 4);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int max_pointer = ByteBuffer.wrap(counter).getInt();
		return max_pointer;
	}
	
	/**
	 * @param chunk
	 * @param current_index
	 * @param rec
	 * @param readPrev
	 * @return pointer_int (data's offset) or -1 if past pointer array
	 */
	private int readChunkData(String chunk, int current_index, TinyRec rec, boolean readPrev, FileHandle ofh){
		int max_pointer = max_pointer(chunk);
		if (DEBUG_SEEK) System.out.println(">\tREADCHUNKDATA: " + current_index);
		
		///int num_pointers = ByteBuffer.wrap(counter).getInt();
		int pointer_int = getNextPointer(current_index, chunk, max_pointer, readPrev); 
		if (DEBUG_SEEK) System.out.println(">\t>  [" + pointer_int + "] @" + current_index);
		
		if(pointer_int > 0){
			current_index = (readPrev) ? current_index - 4 : current_index + 4;
			if (DEBUG_SEEK) System.out.println(">\t>  " + current_index);
		}
		while(pointer_int < 0){
			if(pointer_int == -1){
				if (DEBUG_SEEK) System.out.println(">\t>  " + pointer_int);
				break;
			}
			current_index = (readPrev) ? current_index - 4 : current_index + 4;
			pointer_int = getNextPointer(current_index, chunk, max_pointer, readPrev);
			if (DEBUG_SEEK) System.out.println(">\t>  [" + pointer_int + "] @" + current_index);
		}
		if(pointer_int == -1){
			if (DEBUG_SEEK) System.out.println(">\tREADCHUNKDATA: returned early pointer = -1");
			return pointer_int;
		}
		
		int length_payload = payload_length(current_index, chunk, max_pointer);
		
		byte[] data = new byte[length_payload];
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(chunk, "r");
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			if (DEBUG_SEEK) System.out.println(">\t readchunkdata out of bounds? " + pointer_int);
			raf.seek(pointer_int - length_payload);
			raf.read(data, 0, length_payload);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rec.setPayload(data);
		RID rid = new RID();
		rid.chunkName = chunk;
		rid.offst = pointer_int;
		rid.pointer_index = current_index;
		rid.size = length_payload;
		rec.setRID(rid);
		
		if (DEBUG_SEEK) System.out.println(">\tREADCHUNKDATA: chunkname/offst/payload size" + chunk + " " + rid.offst + " " + rid.size);
		
		return pointer_int;
	}

	/**
	 * 
	 * @param current_index
	 * @param chunk
	 * @param max_pointers
	 * @return payload_length
	 */
	private int payload_length(int current_index, String chunk, int max_pointers){
		int data_offset = getNextPointer(current_index + 4, chunk, max_pointers, true);
		int prev_offset = getNextPointer(current_index, chunk, max_pointers, false);
		if (data_offset < 0) data_offset = -1 *data_offset;
		if (prev_offset < 0){
			if(prev_offset != -1){
				prev_offset = -1 * prev_offset;
			} else {
				prev_offset = 4;
			}
		}
		int payload =  data_offset - prev_offset;
		if (DEBUG_SEEK) System.out.println(">\tPAYLOAD_LENGTH: data_offset: " + data_offset + " prev_offset: " + prev_offset + " payload offset: " + payload);
		return payload;
	}
	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	
	
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		if (DEBUG_SEEK) System.out.println(">\tREADLASTRECORD");
		String last_chunk_name = null;
		int max_pointer = max_pointer(ofh.chunks.getLast());
		int i = ofh.chunks.size() - 1;
		while(max_pointer == 0){
			if(i < 0){
				return FSReturnVals.BadHandle;
			}
			max_pointer = max_pointer(ofh.chunks.get(i));
			i--;
		}
		if(max_pointer > 0){
			last_chunk_name = ofh.chunks.get(i);
		}
		
		int status = readChunkData(last_chunk_name, MAX_CHUNK_SIZE - (max_pointer + 1) * 4, rec, false, ofh);
		if(status == -1){
			return FSReturnVals.Fail;
		}
		return FSReturnVals.Success;
		
		
	}
	
	/**
	 * 
	 * @param current_index
	 * @param chunk
	 * @param max_pointers
	 * @param readPrev (-= 4 if true, += 4 if true)
	 * @return data_pointer
	 * 
	 */
	private int getNextPointer(int current_index, String chunk, int max_pointers, boolean readPrev){
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(chunk, "r");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (DEBUG_SEEK) System.out.print(">\tGETNEXTPOINTER: " + current_index);
		if(readPrev){
			current_index -= 4;
			if(current_index < MAX_CHUNK_SIZE - 4 * max_pointers){
				if (DEBUG_SEEK) System.out.println("\tpast pointer array");
				return -1;
			}
		}else{
			current_index += 4;
			if (DEBUG_SEEK){
				System.out.print(" curr_index + 4 = " + current_index);
				//System.out.println(">\tmax pointers: " + max_pointers);
			}
			if(current_index >= MAX_CHUNK_SIZE){
				if (DEBUG_SEEK) System.out.println("\tpast max size");
				return -1;
			}
		}
		
		byte[] data = new byte[4];
		if(raf != null){
			try {
				raf.seek(current_index);
				raf.read(data, 0, 4);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		int data_pointer = ByteBuffer.wrap(data).getInt();
		if (DEBUG_SEEK) System.out.println("\treturns " + data_pointer);
		return data_pointer;
	}

	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec, int counter) {
		System.out.print(">\t\t" + counter);
		return ReadNextRecord(ofh, pivot, rec);
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
		System.out.println(">\tREADNEXTRECORD: " + pivot.chunkName + " pointer: " + pivot.pointer_index);
		int status = -1;
		
		String chunk = pivot.chunkName;
		int max_pointer = max_pointer(chunk);
		
		if (pivot.pointer_index == (MAX_CHUNK_SIZE - 4 * max_pointer)) {
			// read first item in next chunk
			int next_chunk_index = ofh.chunks.indexOf(chunk) + 1;
			if ((next_chunk_index != 0) && (next_chunk_index < ofh.chunks.size())) {
				String next_chunk = ofh.chunks.get(next_chunk_index);
				status = readChunkData(next_chunk, MAX_CHUNK_SIZE, rec, true, ofh);
			}
		} else {
			status = readChunkData(pivot.chunkName, pivot.pointer_index, rec, true, ofh);
			
		}
		
		return (status == -1) ? FSReturnVals.Fail : FSReturnVals.Success;
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
		//int status = readChunkData(pivot.chunkName, pivot.pointer_index, rec, false, ofh);
		
		int status = -1;
		
		String chunk = pivot.chunkName;
		int max_pointer = max_pointer(chunk);
		
		if (pivot.pointer_index == MAX_CHUNK_SIZE-4) {
			// read first item in next chunk
			int next_chunk_index = ofh.chunks.indexOf(chunk) - 1;
			if ((next_chunk_index >= 0)) {
				String next_chunk = ofh.chunks.get(next_chunk_index);
				int max_pointer_next = max_pointer(next_chunk);
				status = readChunkData(next_chunk, MAX_CHUNK_SIZE - 4 * max_pointer_next -4, rec, false, ofh);
			}
		} else {
			status = readChunkData(pivot.chunkName, pivot.pointer_index, rec, false, ofh);
		}
		
		return (status == -1) ? FSReturnVals.Fail : FSReturnVals.Success;	
	}

	private class ChunkServerThread extends Thread{
		public static final boolean DEBUG_THREAD = true;
		private boolean CNX_MASTER = false; //false if CNX_CLIENT
		
		private ChunkServer cs;
		private Socket s;
		private ObjectInputStream ois;
		private ObjectOutputStream oos;
		private DataInputStream dis;
		private DataOutputStream dos;
		
		public ChunkServerThread(Socket s2, ChunkServer chunkServer){
			cs = chunkServer;
			s = s2;
			try {
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
				dos = new DataOutputStream(oos);
				dis = new DataInputStream(ois);
				this.start();
			} catch (IOException ioe) {
				System.out.println("error while creating chunkserverthread");
				ioe.printStackTrace();
			}
		}
		
		/**
		 * @return String that was read in from Master
		 * reads an int [length], then reads [length] number of bytes from Master's socket
		 */
		private String readStringFromConnection(){
			int length;
			byte[] str;
			String ans = "";
			try {
				length = dis.readInt(); //length of payload
				str = new byte[length];
				for (int i = 0; i < length; i++) {
					str[i] = dis.readByte();
				}
				ans = new String(str, Charset.forName("UTF-8"));
			} catch (IOException ioe){
				ioe.printStackTrace();
			}
			return ans;
		}
		
		private void writeStringToConnection(String payload) {
			int payload_length;
			byte[] payload_bytes = payload.getBytes(Charset.forName("UTF-8"));
			
			payload_length = payload_bytes.length;
			
			try{
				dos.writeInt(payload_length);
				dos.write(payload_bytes, 0, payload_length);
				dos.flush();
				oos.flush();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		public boolean isConnectedtoMaster() { return CNX_MASTER; }
		public boolean isConnectedtoClient() { return !CNX_MASTER; }
		
		public void run() {
			if(ss == null || s == null) return;
			char cmd = 0;
			
			// connect to master
			try {
				cmd = dis.readChar();
				if (cmd == Master.IS_SERVER) {
					// send the chunkserver info
					dos.writeInt(cs.port);
					writeStringToConnection(cs.host);
					// send the files TODO
					CNX_MASTER = true;
				} else {
					CNX_MASTER = false;
				}
			
			cmd = 0;
			while (true) {
				try {
					cmd = dis.readChar();
					if (DEBUG_THREAD) System.out.println("received req:" + cmd);
					switch(cmd){
					case('1'): // APPEND RECORD
						if (DEBUG_THREAD) System.out.println("APPENDRECORD: " + cmd);
						break;
					case('2'): // DELETE RECORD
						if (DEBUG_THREAD) System.out.println("DELETERECORD: " + cmd);
						break;
					case('3'): // READ FIRST RECORD
						if (DEBUG_THREAD) System.out.println("READFIRSTRECORD: " + cmd);
						break;
					case('4'): // READ LAST RECORD
						if (DEBUG_THREAD) System.out.println("READLASTRECORD: " + cmd);
						break;
					case('5'): // READ NEXT RECORD
						if (DEBUG_THREAD) System.out.println("READNEXTRECORD: " + cmd);
						break;
					case('6'): // READ PREV RECORD
						if (DEBUG_THREAD) System.out.println("READPREVRECORD: " + cmd);
						break;
					default:
						if (DEBUG_THREAD) System.out.println("received other request: [ " + cmd + " ]");
						break;
					} /*end switch(cmd)*/
				} catch (IOException ioe) {
					try {
						dos.close(); dis.close(); 
						if (DEBUG_THREAD) System.out.println("Closed dos/ios");
					} catch (IOException ioe2) {
						if (DEBUG_THREAD) System.out.println("error closing dos/ios in this thread");
					}
					break;
				}
			} /*endwhile*/
		}
	}
	
	public static void main(String args[])
	{
	}
}
