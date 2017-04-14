package UnitTests;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * UnitTest3 for Part 1 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */

public class UnitTest3 {

    /**
     * This unit test creates 1000 chunks
     * Each chunk is 4K in size
     * We store an integer in each chunk, reflecting its order in the file:  Chunk0, Chunk1,...., Chunk999
     * Next, we read each chunk.
     * We verify that the content of each chunk has 1024 integers, reflecting the order in which the chunk was created
     */
    final static int NumChunks=1000;
    static String MyChunks[] = new String[NumChunks];  //This array maintains chunk handles that are created
    
    public static void main(String[] args) {
        String ChunkHandle;
        
        Client client = new Client();
        //Write the chunk and the byte array
        int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		int num = ChunkServer.ChunkSize / intSize;	//1024 integers
        for (int i=0; i < NumChunks; i++){
        	byte[] payload = new byte[ChunkServer.ChunkSize];
            byte[] ValInBytes = ByteBuffer.allocate(intSize).putInt(i).array();
            for (int j=0; j < num; j++){
                for (int k=0; k < intSize; k++)
                    payload[(j * intSize) + k] = ValInBytes[k];
            }
            //Create the byte array
            //Create the chunk and store its handle in MyChunks[i]
            MyChunks[i] = client.createChunk();
            boolean isWritten = client.writeChunk(MyChunks[i], payload, 0);
            if(isWritten == false){
            	System.out.println("Unit test 3 result: fail to write a chunk!");
            	return;
            }
        }
        
        for (int i=0; i < NumChunks; i++){
            ChunkHandle = MyChunks[i];
            //ReadChunk using ChunkHandle to get an array of bytes
            byte[] data = new byte[ChunkServer.ChunkSize];
            data = client.readChunk(ChunkHandle, 0, ChunkServer.ChunkSize);
            byte[] ValInBytes = ByteBuffer.allocate(intSize).putInt(i).array();
            //Verify that the content of the array of bytes matches the value i
            for (int j=0; j < num; j++){
                for (int k=0; k < intSize; k++){
                	if(data[j * intSize + k] != ValInBytes[k]){
                		System.out.println("Unit test 3 result: fail!");
                		return;
                	}
                }
            }
        }
        System.out.println("Unit test 3 result: success!");            

    }

}