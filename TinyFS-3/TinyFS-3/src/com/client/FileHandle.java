package com.client;
import java.util.LinkedList;
public class FileHandle {

	public LinkedList<String> chunks = new LinkedList<String>();
	public String identifier;
	public String fileName;
	public String fileDir;
	boolean open;
	
	public FileHandle () {
	}
	
	public void setFilepath(String fp) { identifier = fp; }
	public void setFilename(String fn) { fileName = fn; }
	public void setFiledir(String fd) { fileDir = fd; }
	
	public String getFilepath() { return identifier; }
	public String getFilename() { return fileName; }
	public String getFiledir() { return fileDir; }
	
	public void setOpen(boolean open) { this.open = open; }
}
