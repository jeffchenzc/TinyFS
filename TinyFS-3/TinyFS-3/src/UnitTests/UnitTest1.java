package UnitTests;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * UnitTest1 for Part 1 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */

public class UnitTest1 {
	
	/**
     * This unit test generates an array of 4K bytes where each byte = "1"
     */
    public static String handle = "1";
	
	public static void main(String[] args) {
		test1();
	}
	
	public static void test1(){
		//Write the chunk and the byte array
		byte[] payload = new byte[ChunkServer.ChunkSize];
		int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		int num = ChunkServer.ChunkSize / intSize;	//1024 integers
		byte[] ValInBytes = ByteBuffer.allocate(intSize).putInt(1).array();
        for (int j=0; j < num; j++){
            for (int k=0; k < intSize; k++)
                payload[(j * intSize)+k] = ValInBytes[k];
        }
        boolean isSuccess = false;
        //Create the chunk and store its handle
        Client client = new Client();
        handle = client.createChunk();
        if(handle == null){
        	System.out.println("Unit test 1 result: fail!");
        	return;
        }
       isSuccess = client.writeChunk(handle, payload, 0);
        if(isSuccess == true){
        	System.out.println("Unit test 1 result: success!");
        }else{
        	System.out.println("Unit test 1 result: fail!");
        }
	}

}
