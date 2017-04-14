package com.interfaces;

import java.io.File;

/**
 * Interfaces of the CSCI 485 TinyFS ChunkServer
 * @author Shahram Ghandeharizaden and Jason Gui
 *
 */
public interface ChunkServerInterface {

	public static final int ChunkSize = 4 * 1024; //4 KB chunk sizes
	
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
