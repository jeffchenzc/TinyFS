package UnitTests;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * UnitTest2 for Part 1 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */

public class UnitTest2 {

	/**
     * This unit test call unit test 1, read the chunk and check if every byte = "1"
     */
    
	public static void main(String[] args) {
		UnitTest1 ut1 = new UnitTest1();
		ut1.test1();
		Client client = new Client();
		String handle = ut1.handle;
		byte[] ValInBytes = ByteBuffer.allocate(4).putInt(1).array();
		byte[] data = new byte[ChunkServer.ChunkSize];
        data = client.readChunk(handle, 0, ChunkServer.ChunkSize);
        
        //Verify that the content of the array of bytes matches the value 1
        for (int j=0; j < 1024; j++){
            for (int k=0; k < 4; k++){
            	if(data[j * 4 + k] != ValInBytes[k]){
            		System.out.println("Unit test 2 result: fail!");
            		return;
            	}
            }
        }
        System.out.println("Unit test 2 result: success!");   

	}

}
