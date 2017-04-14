package UnitTests;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;

import com.chunkserver.ChunkServer;
import com.client.Client;

/**
 * UnitTest1 for Part 5 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */

public class UnitTest5 {

	/**
	 * This unit test uses a file (e.g., an image) and stores it as a sequence of chunks,
	 * then reads the chunks and stores them in a file at the client side.
	 * In the end, it verifies that the original matches what you just retrieved.
	 */
    static String[] MyChunks;  //This array maintains chunk handles that are created
    
    static String inputFile = "obama.jpg";
    static String outputFile = "output.jpg";
    
	public static void main(String[] args) throws Exception {
		//read a physical file
		File fin = new File(inputFile);
		if (!fin.exists()) {
			System.out.println("Error:  The input file named "+inputFile+" doesn't exist!");
			System.out.println("Make sure it exists before running this unit test.");
			System.out.println("UnitTest5 failed!");
			return;
		}
		int lastChunkSize = (int) (fin.length() % ChunkServer.ChunkSize);
		// create and write chunk(s) of the file
		Client client = new Client();
		TestReadAndWrite trw = new TestReadAndWrite();
		MyChunks = trw.createFile(fin);
		File fout = new File(outputFile);
		FileOutputStream output = new FileOutputStream(fout);
		for(int i = 0; i < MyChunks.length; i++){
			byte[] data = null;
			if(i != MyChunks.length - 1){
				data = client.readChunk(MyChunks[i], 0, ChunkServer.ChunkSize);
				output.write(data, 0, data.length);
			}else{
				data = client.readChunk(MyChunks[i], 0, lastChunkSize);
				output.write(data, 0, lastChunkSize);
			}
		}
		output.close();
		//compare the contents of both files and determine whether they are the same
		boolean isSame = FileUtils.contentEquals(fin, fout);
		if(isSame == true){
        	System.out.println("Unit test 5 result: success!");
        }else{
        	System.out.println("Unit test 5 result: fail!");
        }
	}
	
}
