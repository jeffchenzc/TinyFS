package com.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	static final boolean DEBUG_SEEK = true;
	static final int MAX_CHUNK_SIZE = 4096;

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

		return FSReturnVals.Success;
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
			AppendRecordToNewChunk(ofh, payload, RecordID);
		}
		String chunk = ofh.chunks.getLast();
		int max_pointer = max_pointer(chunk);
		if (DEBUG_SEEK) System.out.println(">\t max pointers: " + max_pointer);
		byte[] last_offset = new byte[4];
		try {
			raf = new RandomAccessFile(ofh.chunks.getLast(),"rw");
			raf.seek(MAX_CHUNK_SIZE - 4*max_pointer);
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
			return FSReturnVals.Success;
		}
		byte[] number = ByteBuffer.allocate(4).putInt(max_pointer + 1).array();
		byte[] pointer = ByteBuffer.allocate(4).putInt(payload.length+last_offset_int).array();
		try {
			if (DEBUG_SEEK) System.out.println(">\tincrement max pointer: " + (max_pointer+1));
			raf.seek(0);
			raf.write(number, 0, 4);
			raf.seek(last_offset_int);
			raf.write(payload, 0, payload.length);
			raf.seek(MAX_CHUNK_SIZE-4*max_pointer-4);
			raf.write(pointer, 0, 4);
			raf.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
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
			if(current_offset < 0){
				return FSReturnVals.RecDoesNotExist;
			}
			if(current_offset == offst){
				int new_offset = current_offset * -1;
				byte[] write = ByteBuffer.allocate(4).putInt(new_offset).array();
				try {
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
		int current_index = MAX_CHUNK_SIZE - 4;
		
		System.out.println(">\tREADFIRSTRECORD: " + first_chunk_name + " " + current_index);
		int status = readChunkData(first_chunk_name, current_index, rec, false);
		for (int i = 0; i < ofh.chunks.size(); i++) {
			if (status != -1) return FSReturnVals.Success;
			if (status == -1) status = readChunkData(ofh.chunks.get(i), current_index, rec, false);
		}

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
			raf.read(counter, 0, 4);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int max_pointer = ByteBuffer.wrap(counter).getInt();
		return max_pointer;
	}
	
	private int readChunkData(String chunk, int current_index, TinyRec rec, boolean readPrev){
		int max_pointer = max_pointer(chunk);
		
		///int num_pointers = ByteBuffer.wrap(counter).getInt();
		int pointer_int = getNextPointer(current_index, chunk, max_pointer, readPrev); 
		while(pointer_int < 0){
			if(pointer_int == -1){
				break;
			}
			current_index = (readPrev) ? current_index - 4 : current_index + 4;
			pointer_int = getNextPointer(current_index, chunk, max_pointer, readPrev);
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
			int data_length = raf.read(data, pointer_int, length_payload);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rec.setPayload(data);
		RID rid = new RID();
		rid.chunkName = chunk;
		rid.offst = pointer_int;
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
		int prev_offset = getNextPointer(current_index, chunk, max_pointers, true);
		if(prev_offset < 0){
			prev_offset = -1 * prev_offset;
		}
		int payload =  prev_offset - data_offset;
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
		String last_chunk_name = null;
		int max_pointer = max_pointer(ofh.chunks.getLast());
		int i = ofh.chunks.size()-1;
		while(max_pointer == 0){
			i--;
			if(i < 0){
				return FSReturnVals.BadHandle;
			}
			max_pointer = max_pointer(ofh.chunks.get(i));
		}
		if(max_pointer > 0){
			last_chunk_name = ofh.chunks.get(i);
		}
		
		int status = readChunkData(last_chunk_name, MAX_CHUNK_SIZE - max_pointer * 4, rec, true);
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
			if(current_index > MAX_CHUNK_SIZE){
				if (DEBUG_SEEK) System.out.println("past max size");
				return -1;
			}
		}else{
			current_index += 4;
			if (DEBUG_SEEK){
				System.out.print(" curr_index + 4 = " + current_index);
				System.out.println(" <? " + (MAX_CHUNK_SIZE - (4 * max_pointers)));
				System.out.println(">\t max pointers: " + max_pointers);
			}
			if(current_index < MAX_CHUNK_SIZE - 4 * max_pointers){
				if (DEBUG_SEEK) System.out.println(">\t GETNEXTPOINTER: past pointer array");
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
		System.out.println(">\t GETNEXTPOINTER: returns " + data_pointer);
		return data_pointer;
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
		System.out.println(">\tREADNEXTRECORD: " + pivot.chunkName + " offset: " + pivot.offst);
		int status = readChunkData(pivot.chunkName, pivot.offst, rec, false);
		
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
		int status = readChunkData(pivot.chunkName, pivot.offst, rec, true);
		
		return (status == -1) ? FSReturnVals.Fail : FSReturnVals.Success;	
	}

}
