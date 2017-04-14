package UnitTests;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Ref;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * A utility used by the UnitTests
 * @author Shahram Ghandeharizaden and Jason Gui
 *
 */

public class TestReadAndWrite {
	
	public static ChunkServer cs = new ChunkServer();
	public static Client client = new Client();
	
	/**
	 * Create and write chunk(s) of a physical file.
	 * The default full chunk size is 4K. Note that the last chunk of the file may not have the size 4K.
	 * The sequence of chunk handles are returned, which are stored in a static map.
	 */
	public String[] createFile(File f) {
		try {
			RandomAccessFile raf = new RandomAccessFile(f.getAbsolutePath(), "rw");
			raf.seek(0);
			long size = f.length();
			int num = (int)Math.ceil((double)size / cs.ChunkSize);
			String[] ChunkHandles = new String[num];
			String handle = null;
			byte[] chunkArr = new byte[cs.ChunkSize];
			for(int i = 0; i < num; i++){
				handle = client.createChunk();
				ChunkHandles[i] = handle;
				raf.read(chunkArr, 0, cs.ChunkSize);
				boolean isWritten = client.writeChunk(handle, chunkArr, 0);
				if(isWritten == false){
					throw new IOException("Cannot write a chunk to the chunk server!");
				}
			}
			raf.close();
			return ChunkHandles;
		} catch (IOException ie){
			return null;
		}
	}

}
