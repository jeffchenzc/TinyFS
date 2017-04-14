package UnitTests;

import java.io.File;

import com.client.Client;

/**
 * UnitTest4 for Part 1 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */

public class UnitTest4 {

	/**
	 * This unit test uses a file (e.g., an image) and stores it as a sequence of chunks
	 */
    static String[] MyChunks;  //This array maintains chunk handles that are created
    
	public static void main(String[] args) {
		//read a physical file
		String filePath = "obama.jpg";
		File fin = new File(filePath);
		if(!fin.exists()){
			System.out.println("The file doesn't exist!");
		}
		//create and write chunk(s) of the file
		TestReadAndWrite trw = new TestReadAndWrite();
		MyChunks = trw.createFile(fin);
		if(MyChunks != null){
        	System.out.println("Unit test 4 result: success!");
        }else{
        	System.out.println("Unit test 4 result: fail!");
        }
	}

}
