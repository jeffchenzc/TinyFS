package com.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.Charset;

import com.chunkserver.*;

public class ClientFS {
	
	private static final boolean DEBUG_DETAIL = true;
	private static final boolean DEBUG_MASTER_CNX = true;
	
	Master mas = new Master();

	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail, //Returned when a method fails
		
		ReadToNextChunk
	}
	
	private Socket master_s;
	private ObjectOutputStream master_oos;
	private ObjectInputStream master_ois;
	private DataOutputStream master_dos;
	private DataInputStream master_dis;

	private static String host = "localhost";
	private static int port = 8888;
	private static int int_size = Integer.SIZE/Byte.SIZE;
	
	/**
	 * Initialize the client
	 */
	public ClientFS(){
		master_s = null;
		System.out.println("Create ClientFS");
		try {
			master_s = new Socket(host, port);
			master_oos = new ObjectOutputStream(master_s.getOutputStream());
			master_ois = new ObjectInputStream(master_s.getInputStream());
			master_dos = new DataOutputStream(master_oos);
			master_dis = new DataInputStream(master_ois);
		} catch (IOException ioe) {
			System.out.println("Connection refused: " + host + ":" + port);
			try {
				if (master_s != null) master_s.close();
			} catch (IOException ioe2) {
				System.out.println("IOE when closing connection to " + host + ":" + port);
			}
		}
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
			if (DEBUG_MASTER_CNX) System.out.println("received len " + length);
			str = new byte[length];
			for (int i = 0; i < length; i++) {
				str[i] = master_dis.readByte();
			}
			ans = new String(str, Charset.forName("UTF-8"));
			if (DEBUG_MASTER_CNX) System.out.println("response: [" + length + ": " + ans + "]");
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
	
	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		int length;
		byte[] str;
		FSReturnVals v;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to create dir " + src + dirname);
			return null;
		}
		try{
			master_dos.writeChar(Master.CREATEDIR);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: create dir " + src + dirname );
			writeStringToMaster(src);
			writeStringToMaster(dirname);
			v = FSReturnVals.valueOf(readStringFromMaster());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*
		FSReturnVals temp = mas.createDir(src, dirname);
		if (DEBUG_DETAIL) System.out.println(temp.toString());*/
		return v;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		int length;
		byte[] str;
		FSReturnVals v;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to create dir " + src + dirname);
			return null;
		}
		try{
			master_dos.writeChar(Master.CREATEDIR);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: delete dir " + src + dirname );
			writeStringToMaster(src);
			writeStringToMaster(dirname);
			v = FSReturnVals.valueOf(readStringFromMaster());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*FSReturnVals v = mas.deleteDir(src, dirname);
		if (DEBUG_DETAIL) System.out.println("delete: " + v.toString());
		*/
		return v;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		int length;
		byte[] str;
		FSReturnVals v;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to delete dir " + src + NewName);
			return null;
		}
		try{
			master_dos.writeChar(Master.DELETEDIR);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: delete dir " + src + NewName );
			writeStringToMaster(src);
			writeStringToMaster(NewName);
			v = FSReturnVals.valueOf(readStringFromMaster());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*
		FSReturnVals v = mas.renameDir(src, NewName);
		if (DEBUG_DETAIL) System.out.println("rename returns: " + v.toString());
		*/
		return v;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		int dirs_length;
		byte[] str;
		String[] dirs;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to list dir " + tgt);
			return null;
		}
		try{
			master_dos.writeChar(Master.LISTDIR);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: list dir " + tgt );
			writeStringToMaster(tgt);
			dirs_length = master_dis.readInt();
			dirs = new String[dirs_length];
			for (int i = 0; i < dirs_length; i++) {
				dirs[i] = readStringFromMaster();
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*return mas.listDir(tgt);*/
		return dirs;
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		int length;
		byte[] str;
		FSReturnVals v;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to create file " + tgtdir + filename);
			return null;
		}
		try{
			master_dos.writeChar(Master.CREATEFILE);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: create file " + tgtdir + filename );
			writeStringToMaster(tgtdir);
			writeStringToMaster(filename);
			v = FSReturnVals.valueOf(readStringFromMaster());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*
		FSReturnVals v = mas.CreateFile(tgtdir, filename);
		if (DEBUG_DETAIL) System.out.println("createfile " + tgtdir + filename + " returns: " + v.toString());
		*/
		return v;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		int length;
		byte[] str;
		FSReturnVals v;
		if (master_s == null) {
			if (DEBUG_MASTER_CNX) System.out.println("socket is null; fail to del dir " + tgtdir + filename);
			return null;
		}
		try{
			master_dos.writeChar(Master.CREATEDIR);
			master_dos.flush();
			master_oos.flush();
			if (DEBUG_MASTER_CNX) System.out.println("REQUEST: del dir " + tgtdir + filename );
			writeStringToMaster(tgtdir);
			writeStringToMaster(filename);
			v = FSReturnVals.valueOf(readStringFromMaster());
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return null; //abort
		}
		
		/*
		FSReturnVals v = mas.DeleteFile(tgtdir, filename);
		if (DEBUG_DETAIL) System.out.println("deletefile " + tgtdir + filename + " returns: " + v.toString());
		*/
		return v;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		FSReturnVals v = mas.OpenFile(FilePath, ofh);
		if (DEBUG_DETAIL) System.out.println("openfile " + ofh.getFilepath() + " returns: " + v.toString());
		return v;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		FSReturnVals v = mas.CloseFile(ofh);
		if (DEBUG_DETAIL) System.out.println("closefile " + ofh.getFilepath() + " returns: " + v.toString());
		return v;
	}

}
