package com.chunkserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Pattern;

import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;
import java.util.LinkedList;

public class Master {
   
	private static boolean DEBUG_SERVER = false;
	private static boolean DEBUG_RENAME = false;
	private static boolean DEBUG_THREAD = true;
	
	private ServerSocket ss;
	private Socket s;
	private Vector<ChunkServerThread> threads;
	private static int port = 8888;
	private static int int_size = Integer.SIZE/Byte.SIZE;
	
	Map<String, LinkedList<String> > path = new HashMap<String, LinkedList<String> >();
	Map<String, LinkedList<FileHandle> > file = new HashMap<String, LinkedList<FileHandle> >();
	
	public static final char CREATEDIR	= '1';
	public static final char DELETEDIR	= '2';
	public static final char RENAMEDIR	= '3';
	public static final char LISTDIR	= '4';
	public static final char CREATEFILE	= '5';
	public static final char DELETEFILE	= '6';
	public static final char OPENFILE	= '7';
	public static final char CLOSEFILE	= '8';
	
	public Master(){
		path.put("/", new LinkedList<String>());
		
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
	
	/**
	 * @param source
	 * @param subpath
	 * @return
	 */
	private String getDirKey(String source, String subpath) {
		if (subpath == "") return source + "/";
		return source + subpath + "/";
	}
	
	/**
	 * @param path
	 * @return
	 */
	private String[] getDirSubdir (String path) {
		String[] dir = new String[2];
		String[] splitstr = path.split("/");
		String parentdir = "";

		if (DEBUG_RENAME) System.out.println(">\tGetDirSubdir:");
		for (int i = 0; i < splitstr.length - 1; i++) {
			if (splitstr[i].equals("")) continue;
			parentdir += "/" + splitstr[i];
			if (DEBUG_RENAME) System.out.println(">\t" + splitstr[i]);
		}
		parentdir += "/";
		if (DEBUG_RENAME) System.out.println(">\t" + parentdir);
		
		dir[0] = parentdir;
		dir[1] = splitstr[splitstr.length - 1];
		
		return dir;
	}
	
	public FSReturnVals createDir(String source, String subpath){
		if(path.containsKey(source)){
			if(path.get(source).indexOf(subpath) == -1){
				path.get(source).add(subpath);
				path.put(getDirKey(source, subpath), new LinkedList<String>());
				return FSReturnVals.Success;
			}
			return FSReturnVals.DestDirExists;
		}
		return FSReturnVals.SrcDirNotExistent;
	}
	
	public FSReturnVals deleteDir(String source, String subpath){
		System.out.println("delete from src " + source + " " + subpath);
		if(path.containsKey(source)){
			if(path.get(source).indexOf(subpath) == -1){
				return FSReturnVals.SrcDirNotExistent;
			}
			if (path.get(getDirKey(source,subpath)).size() > 0 )
				return FSReturnVals.DirNotEmpty;
			
			path.get(source).remove(subpath);
			path.remove(getDirKey(source,subpath));
			return FSReturnVals.Success;
		}
		System.out.println("delete: source: " + source);
		return FSReturnVals.SrcDirNotExistent;
	}
	
	/*
	 * find original dir (eg "/a/b/c")
	 * rename original and copy over children.
	 * rename each child (full child key, full original, full rename)
	 */
	public FSReturnVals renameDir(String original, String rename){
		
		String[] splitOriginal = getDirSubdir(original);
		String parentDir = splitOriginal[0];
		String renameThisDir = splitOriginal[1];
		String renameToThis = getDirSubdir(rename)[1];
		LinkedList<String> children, parent;
		
		if (DEBUG_RENAME) System.out.println("rename " + original + " to " + rename);
		
		if(path.containsKey(parentDir)){
			parent = path.get(parentDir);
			if(parent.indexOf(renameToThis) != -1){
				if (DEBUG_RENAME) System.out.println("rename to: " + rename);
				return FSReturnVals.DestDirExists;
			}
			
			recurRenameChild(getDirKey(original,""), original, rename);
			
			return FSReturnVals.Success;
		}
		
		if (DEBUG_RENAME) System.out.println("rename failed to find file:" + parentDir);
		return FSReturnVals.SrcDirNotExistent;
	}
	
	/* 
	 * rename the given parentDirPath by replacing the "oldName" substring to "newName"
	 * and call recurRename on all of the parentDirPath's children.
	 */
	public void recurRenameChild(String parentDirPath, String oldDirPath, String newDirPath){
		LinkedList<String> children;
		String renamedParent, childToRename;
		
		if (DEBUG_RENAME) System.out.println("rename children of: " + parentDirPath);
		children = path.get(parentDirPath);
		renamedParent = parentDirPath.replaceFirst(oldDirPath, newDirPath);
		path.put(renamedParent, children);
		
		for (int i = 0; i < children.size(); i++) {
			childToRename = getDirKey(parentDirPath, children.get(i).toString());
			recurRenameChild(childToRename, oldDirPath, newDirPath);
		}
		
		path.remove(parentDirPath);
	}
	
	public String[] listDir(String target){ 
		if(path.get(target+"/") == null){
			System.out.println("Cannot list dir: " + target + " linked list is fucking null");
			return new String[0];
		}
		return recurList(target);
	}
	
	//recursive, key has no "/" at the end
	public String[] recurList(String key) {
		LinkedList<String> subdirs = path.get(getDirKey(key,""));
		System.out.println(getDirKey(key,"") + " " + subdirs);
		String[] list = new String[subdirs.size()];
		String[] newlist = new String[0];
		String[] returnedList;
		
		if (subdirs == null) return newlist;
		
		//get list
		list = subdirs.toArray(new String[0]);
		for (int ii = 0; ii < list.length; ii++) {
			list[ii] = key + "/" + list[ii];
		}
		
		//get newlist
		for(int i = 0; i < subdirs.size(); i++) {
			newlist = recurList(key + "/" + subdirs.get(i));
		}
		
		//append list and newlist
		returnedList = new String[list.length + newlist.length];
		for(int j = 0; j < list.length; j++) {
			returnedList[j] = list[j];
		}
		for(int k = list.length; k < returnedList.length; k++) {
			returnedList[k] = newlist[k - list.length];
		}
		
		return returnedList;
	}
	
	public FSReturnVals CreateFile (String tgtdir, String filename) {
		FileHandle fh = null;
		//add stuff to filehandle
		
		if (path.containsKey(tgtdir)){
			//if tgtdir has files under it
			if (file.containsKey(tgtdir)) {
				fh = getFileHandle(filename, file.get(tgtdir)); 
				if (fh != null) {
					return FSReturnVals.FileExists;
				}
				fh = new FileHandle();
				AddFHData(tgtdir, filename, fh);
				file.get(tgtdir).add(fh);
				return FSReturnVals.Success;
			}
			fh = new FileHandle();
			file.put(tgtdir, new LinkedList<FileHandle>());
			AddFHData(tgtdir, filename, fh);
			file.get(tgtdir).add(fh);
			return FSReturnVals.Success;
		}
		return FSReturnVals.SrcDirNotExistent;
	}
	
	private void AddFHData (String tgtdir, String filename, FileHandle fh) {
		fh.setFilename(filename);
		fh.setFiledir(tgtdir);
		fh.setFilepath(tgtdir+filename);
		fh.setOpen(false);
	}
	
	public FSReturnVals DeleteFile (String tgtdir, String filename) {
		FileHandle fh;
		
		if (path.containsKey(tgtdir)){
			//if tgtdir has files under it
			if (file.containsKey(tgtdir)) {
				fh = getFileHandle(filename, file.get(tgtdir)); 
				if (fh != null) {
					file.get(tgtdir).remove(fh);
					return FSReturnVals.Success;
				}
			}
			return FSReturnVals.FileDoesNotExist;
		}
		return FSReturnVals.SrcDirNotExistent;
	}
	
	public FSReturnVals OpenFile (String filepath, FileHandle ofh) {
		FileHandle fh;
		String[] parsefp = getDirSubdir(filepath);
		String tgtdir = parsefp[0];
		String filename = parsefp[1];
		
		if (path.containsKey(tgtdir)){
			//if tgtdir has files under it
			if (file.containsKey(tgtdir)) {
				fh = getFileHandle(filename, file.get(tgtdir)); 
				if (fh != null) {
					//populate the filehandle ofh
					ofh.setFiledir(tgtdir);
					ofh.setFilename(filename);
					ofh.setFilepath(tgtdir+filename);
					ofh.setOpen(true);
					//ofh = fh;
					return FSReturnVals.Success;
				}
			}
			return FSReturnVals.FileDoesNotExist;
		}
		return FSReturnVals.SrcDirNotExistent;
	}
	
	public FSReturnVals CloseFile (FileHandle cfh) {
		FileHandle fh;
		//String[] parsefp = getDirSubdir(cfh.getFilepath()); /*TODO FileHandle.filepath*/
		String tgtdir = cfh.getFiledir();
		String filename = cfh.getFilename();
		
		if (path.containsKey(tgtdir)){
			//if tgtdir has files under it
			if (file.containsKey(tgtdir)) {
				fh = getFileHandle(filename, file.get(tgtdir)); 
				if (fh != null)
					fh.setOpen(false);
					cfh = fh;
					return FSReturnVals.Success;	/*TODO: close the filehandle cfh*/
			}
			return FSReturnVals.BadHandle;
		}
		
		//cfh is invalid
		return FSReturnVals.BadHandle;
	}
	
	/*
	 * Linearly searches the linkedList for a FileHandle whos filename
	 * matches the String file. Returns null if no match match, else
	 * returns the FileHandle.
	 */
	private FileHandle getFileHandle(String file, LinkedList<FileHandle> linkedList) {
		FileHandle fh = null;
		for (int i = 0; i < linkedList.size(); i++) {
			fh = linkedList.get(i);
			//System.out.println(">\t[" + i + "] " + fh.getFilepath());
			if (fh.getFilename().equals(file)) return fh; /*TODO FileHandle.filename*/
		}
		return null;
	}
	
	private class ChunkServerThread extends Thread{
		private Master mas;
		private Socket s;
		private ObjectInputStream ois;
		private ObjectOutputStream oos;
		private DataInputStream dis;
		private DataOutputStream dos;
		
		/**
		 * @param s (Socket)
		 * @param master (ChunkServer)
		 */
		public ChunkServerThread(Socket s, Master master) {
			this.s = s;
			this.mas = master;
			try{
				ois = new ObjectInputStream(s.getInputStream());
				oos = new ObjectOutputStream(s.getOutputStream());
				dis = new DataInputStream(ois);
				dos = new DataOutputStream(oos);
				this.start();
			} catch (IOException ioe) {
				if (DEBUG_THREAD) System.out.println("Error while creating thread");
				ioe.printStackTrace();
			}
		}
		
		private String readStringFromClient(){
			int length;
			byte[] str;
			String ans = "";
			try {
				length = dis.readInt(); //length of payload
				//if (DEBUG_SERVER) System.out.println("received len " + length);
				str = new byte[length];
				for (int i = 0; i < length; i++) {
					str[i] = dis.readByte();
				}
				ans = new String(str, Charset.forName("UTF-8"));
				//if (DEBUG_SERVER) System.out.println("response: [" + length + ": " + ans + "]");
			} catch (IOException ioe){
				ioe.printStackTrace();
			}
			return ans;
		}
		
		private void writeStringToClient(String payload) {
			int payload_length;
			byte[] payload_bytes = payload.getBytes(Charset.forName("UTF-8"));
			
			payload_length = payload_bytes.length;
			
			try{
				dos.writeInt(payload_length);
				dos.write(payload_bytes, 0, payload_length);
				dos.flush();
				dos.flush();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		
		private void writeFSValsToClient(FSReturnVals v){
			writeStringToClient(v.toString());
		}
		
		public void run() {
			FSReturnVals v;
			int length;
			if(ss == null || s == null) return;
			char cmd = 0;
			while (true) {
				try {
					cmd = dis.readChar();
					if (DEBUG_THREAD) System.out.println("received req:" + cmd);
					switch(cmd){
					case('1'): // CREATE DIR
						String createdir_src = readStringFromClient();
						String createdir_source = readStringFromClient();
						v = mas.createDir(createdir_src, createdir_source);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("CREATEDIR " + createdir_src + createdir_source);
						break;
					case('2'): // DELETE DIR
						String deldir_src = readStringFromClient();
						String deldir_dirname = readStringFromClient();
						v = mas.deleteDir(deldir_src, deldir_dirname);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("DELETEDIR " + deldir_src + deldir_dirname + " " + v.toString());
						break;
					case('3'): // RENAME DIR
						String rename_tgt = readStringFromClient();
						String rename_newname = readStringFromClient();
						v = mas.renameDir(rename_tgt, rename_newname);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("RENAMEDIR " + rename_tgt + rename_newname + " " + v.toString());
						break;
					case('4'): // LIST DIR
						String list_tgt = readStringFromClient();
						String[] list_dirs = mas.listDir(list_tgt);
						length = list_dirs.length;
						dos.writeInt(length);
						dos.flush();
						for (int listi = 0; listi < length; listi++){
							writeStringToClient(list_dirs[listi]);
						}
						if (DEBUG_THREAD) System.out.println("LISTDIR " + list_tgt);
						break;
					case('5'): // CREATE FILE
						String createfile_tgtdir = readStringFromClient();
						String createfile_filename = readStringFromClient();
						v = mas.CreateFile(createfile_tgtdir, createfile_filename);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("CREATEFILE " + createfile_tgtdir + createfile_filename);
						break;
					case('6'): // DELETE FILE
						String deletefile_tgtdir = readStringFromClient();
						String deletefile_filename = readStringFromClient();
						v = mas.DeleteFile(deletefile_tgtdir, deletefile_filename);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("DELETEFILE " + deletefile_tgtdir + deletefile_filename);
						break;
					case('7'): // OPEN FILE
						String openfilepath = readStringFromClient();
						String ofh_id = readStringFromClient();
						String open_tgtdir = getDirSubdir(openfilepath)[0];
						FileHandle fh = getFileHandle(openfilepath, file.get(open_tgtdir));
						v = mas.OpenFile(openfilepath, fh);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("OPENFILE " + ofh_id);
						break;
					case('8'): // CLOSE FILE
						String cfh_id = readStringFromClient();
						String close_tgtdir = readStringFromClient();
						FileHandle cfh = getFileHandle(cfh_id, file.get(close_tgtdir));
						v = mas.CloseFile(cfh);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("CLOSEFILE " + cfh_id);
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
	
	public static void main(String[] args){
		Master mas = new Master();
	}

}

