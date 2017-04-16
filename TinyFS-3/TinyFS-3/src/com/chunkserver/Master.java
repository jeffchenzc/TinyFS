package com.chunkserver;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.client.FileHandle;
import com.client.ClientFS.FSReturnVals;
import java.util.LinkedList;

public class Master {
   
	Map<String, LinkedList<String> > path = new HashMap<String, LinkedList<String> >();
	Map<String, FileHandle> file = new HashMap<String, FileHandle>();
	
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

		for (int i = 0; i < splitstr.length - 1; i++) {
			parentdir += "/" + splitstr[i];
		}
		parentdir += "/";
		
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
	
	public FSReturnVals renameDir(String original, String rename){
		//find original.
		//rename original and copy over children.
		//rename each child (full child key, full original, full rename)
		
		
		String[] splitOriginal = getDirSubdir(original);
		String parentDir = splitOriginal[0];
		String renameThisDir = splitOriginal[1];
		String renameToThis = getDirSubdir(rename)[1];
		
		if(path.containsKey(parentDir)){
			if(path.get(parentDir).indexOf(renameThisDir) != -1){
				return FSReturnVals.DestDirExists;
			}
			path.get(parentDir).remove(renameThisDir);
			path.get(parentDir).add(renameToThis);
			//rename all the children directories
			
			return FSReturnVals.Success;
		}
		
		return FSReturnVals.SrcDirNotExistent;
	}
	
	public String[] listDir(String target){ 
		if(path.get(target+"/") == null){
			System.out.println(target + "/ linked list is fucking null");
			return new String[0];
		}
		return recurList(target);
	}
	
	public void recurRename(String parent, String oldName, String newSubdirName){
		//follow all children of the given parent dir and rename "oldName" to "newName"
		LinkedList children = path.get(parent);
		for (int i = 0; i < children.size(); i++) {
			rename(children.get(i).toString(), oldName, newSubdirName);
			path.put(key, value);
			recurRename(children.get(i).toString(), oldName, newSubdirName);
		}
		//remove all the old children
		
	}
	
	/* 
	 * search dir to replace the first instance of oldName with newName
	 * in event of failure, return original dir
	 */
	private String rename(String dir, String oldName, String newName){
		String preStr = "";
		String postStr = "";
		boolean post = false;
		String[] splitDir = dir.split("/");
		for (int i = 0; i < splitDir.length; i++) {
			if (splitDir[i].equals(oldName)) 
				post = true;
			if (post) {
				postStr += "/" + splitDir[i];
			}
			preStr += "/" + splitDir[i];
		}
		if (!post) { return dir; }
		return preStr + "/" + newName + postStr + "/";
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
	
	public static void main(String[] args){
		
	}

}

