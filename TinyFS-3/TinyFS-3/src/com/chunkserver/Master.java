package com.chunkserver;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;
import java.util.LinkedList;

public class Master {
   
	private static boolean DEBUG_RENAME = false;
	
	Map<String, LinkedList<String> > path = new HashMap<String, LinkedList<String> >();
	Map<String, LinkedList<FileHandle> > file = new HashMap<String, LinkedList<FileHandle> >();
	
	public Master(){
		path.put("/", new LinkedList<String>());
	}
	
	private String getDirKey(String source, String subpath) {
		if (subpath == "") return source + "/";
		return source + subpath + "/";
	}
	
	private String[] getDirSubdir(String path) {
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
			System.out.println(target + "/ linked list is fucking null");
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
					ofh = fh;
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
	
	public static void main(String[] args){
		
	}

}

