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
   
	private static boolean DEBUG_SERVER = true;
	private static boolean DEBUG_RENAME = false;
	private static boolean DEBUG_THREAD = true;
	
	private ServerSocket ss;
	private Socket s;
	private Vector<ChunkServerThread> threads;
	private static int port = 8888;
	private static int int_size = Integer.SIZE/Byte.SIZE;
	
	Map<String, LinkedList<String> > path = new HashMap<String, LinkedList<String> >();
	Map<String, LinkedList<FileHandle> > file = new HashMap<String, LinkedList<FileHandle> >();
	Map<String, ServerInfo> servers = new HashMap<String, ServerInfo>();
	
	public static final char CREATEDIR	= '1';
	public static final char DELETEDIR	= '2';
	public static final char RENAMEDIR	= '3';
	public static final char LISTDIR	= '4';
	public static final char CREATEFILE	= '5';
	public static final char DELETEFILE	= '6';
	public static final char OPENFILE	= '7';
	public static final char CLOSEFILE	= '8';
	public static final char GETSERVERFORFH = '9';
	public static final char IS_SERVER = 'A';
	public static final char IS_CLIENT = 'B';
	public static final char GET_SERVER_INFO = 'C';
	public static final char CREATECHUNK = 'D';
	
	public ServerInfo getServerInfo() {
		boolean isServer = false;
		for (int i = 0; i < threads.size(); i++) {
			isServer = threads.get(i).isConnectedToServer();
			if (isServer) {
				return threads.get(i).getConnectionInfo();
			}
		}
		return null;
	}
	
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
	
	public static class ServerInfo {
		public String host;
		public int port;
		public ServerInfo(String host, int port) {	
			this.host = host;
			this.port = port;
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
		System.out.println("\t\tfilename in CreateFile " + filename);
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
		fh.setFilepath(tgtdir + filename);
		fh.setOpen(false);
		System.out.println("Set filehandle data: " + tgtdir + " " + filename);
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
		
		if (DEBUG_SERVER) System.out.println("OPENFILE: " + tgtdir + " " + filename + " " + ofh);
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
	 * Linearly searches the linkedList for a FileHandle whose filename
	 * matches the String file. Returns null if no match match, else
	 * returns the FileHandle.
	 */
	private FileHandle getFileHandle(String file, LinkedList<FileHandle> linkedList) {
		FileHandle fh = null;
		//System.out.println(">\t" + file);
		System.out.println("\t\tfile is " + file);
		if(linkedList.isEmpty()){
			System.out.println("\t\tlinkedlist is empty");
		}
		System.out.println("\t\t1st element in linkedlist is " + linkedList.getFirst().getFilename());
		for (int i = 0; i < linkedList.size(); i++) {
			fh = linkedList.get(i);
			//System.out.println(">\t[" + i + "] " + fh.getFilename());
			if(file.contains("/")){
				String[] temp = file.split("/");
				String real_name = temp[temp.length - 1];
				if(fh.getFilename().equals(real_name)){
					return fh;
				}
			}else{
				if (fh.getFilename().equals(file)){ 
					System.out.println("\t\t HIT!!!!");
					return fh; /*TODO FileHandle.filename*/}
			}
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
		
		public boolean CNX_SERVER = false;
		public ServerInfo cnx_info = null;
		
		public boolean isConnectedToServer() { return CNX_SERVER; }
		public boolean isConnectedToClient() { return !CNX_SERVER; }
		
		/**
		 * @return null if ServerInfo not received yet or if this thread is connected to a Client
		 */
		public ServerInfo getConnectionInfo() { return cnx_info; }
		
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
		
		private ServerInfo getServerForFH(String get_fh){
			ServerInfo server_info;
			// there's only one server right now, so return the first thread that's a server.
			server_info = mas.getServerInfo();
			return server_info;
		}
		
		public void run() {
			FSReturnVals v;
			int length;
			
			int cs_port, cs_num_files;
			String cs_host, cs_filename = "";
			ServerInfo cs_info;
			
			if(ss == null || s == null) return;
			
			// if server, store fileID, IP and port.
			try {
				//dos.writeChar(Master.IS_CLIENT);
				//dos.flush();
				char type = dis.readChar();
				if (type == Master.IS_SERVER) {
					if (DEBUG_SERVER) System.out.println("Master connected to a server! Get port and host");
					CNX_SERVER = true;
					dos.writeChar(Master.GET_SERVER_INFO);
					dos.flush();
					// get server info
					cs_port = dis.readInt();
					cs_host = readStringFromClient();
					cs_info = new ServerInfo(cs_host, cs_port);
					cnx_info = cs_info;
					// for each filename received, add pair (filename, server) to servers
					cs_num_files = dis.readInt();
					for (int i = 0; i < cs_num_files; i++) {
						cs_filename = readStringFromClient();
						servers.put(cs_filename, cs_info);
					}
				} else {
					CNX_SERVER = false;
					if (DEBUG_SERVER) System.out.println("Master connected to a client! Continue as normal.");
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
			
			// wait for commands
			char cmd = 0;
			String tgtdir = "";
			while (true) {
				try {
					cmd = dis.readChar();
					if (DEBUG_THREAD) System.out.println("received req:" + cmd);
					switch(cmd){
					case('9'): // GET SERVER FOR FILEHANDLE
						String filehandleID = readStringFromClient();
						String get_tgtdir = getDirSubdir(filehandleID)[0];
						FileHandle get_fh = getFileHandle(filehandleID, file.get(get_tgtdir));
						ServerInfo server_info = getServerForFH(get_fh.identifier);
						dos.writeInt(server_info.port);
						writeStringToClient(server_info.host);
					case('1'): // CREATE DIR
						String createdir_src = readStringFromClient();
						String createdir_source = readStringFromClient();
						v = mas.createDir(createdir_src, createdir_source);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("CREATEDIR " + createdir_src + " " + createdir_source + " " + v.toString());
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
						if (DEBUG_THREAD) System.out.println("CREATEFILE " + createfile_tgtdir + " " + createfile_filename);
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
						String open_tgtdir = getDirSubdir(openfilepath)[0];
						//System.out.println("\topenfilepath is  "+openfilepath);
						FileHandle fh = getFileHandle(openfilepath, file.get(open_tgtdir));
						writeStringToClient(fh.getFilename());
						writeStringToClient(fh.getFilepath());
						writeStringToClient(fh.getFiledir());
						if(fh == null){
							//System.out.println("\t\tfh is NULLLLLLLLLLLLLLL");
						}
						v = mas.OpenFile(openfilepath, fh);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("OPENFILE " + open_tgtdir + " " + openfilepath);
						break;
					case('8'): // CLOSE FILE
						String cfh_id = getDirKey(readStringFromClient(),"");
						String close_tgtdir = readStringFromClient(); //getDirSubdir(cfh_id)[0];
						FileHandle cfh = getFileHandle(cfh_id, file.get(close_tgtdir));
						v = mas.CloseFile(cfh);
						writeFSValsToClient(v);
						if (DEBUG_THREAD) System.out.println("CLOSEFILE " + cfh_id);
						break;
					case('D'): // CREATE CHUNK
						String createchunk_name = readStringFromClient();
						String createchunk_ofh_id = readStringFromClient();
						tgtdir = getDirSubdir(createchunk_ofh_id)[0];
						FileHandle createchunk_fh = getFileHandle(createchunk_ofh_id, file.get(tgtdir));
						createchunk_fh.chunks.add(createchunk_name);
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

