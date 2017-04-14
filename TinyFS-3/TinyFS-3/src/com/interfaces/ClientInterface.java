package com.interfaces;

import java.io.File;
import java.util.HashMap;

/**
 * Interfaces of the TinyFS Client 
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public interface ClientInterface {
	
	
	
	/**
	 * Return the chunkhandle for a newly created chunk.
	 */
	public String createChunk();
	
	/**
	 * Write the byte array payload to the ChunkHandle at the specified offset.
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset);
	
	/**
	 * Read the specified NumberOfBytes from the target chunk starting at the specified offset.
	 * Return the retrieved number of bytes as a byte array.
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes);	
	
}
