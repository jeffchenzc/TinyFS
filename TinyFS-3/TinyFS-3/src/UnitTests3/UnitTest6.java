package UnitTests3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.ClientRec;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;

/**
 * UnitTest6 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest6 {
	static final String TestName = "Unit Test 6: ";

	public static void main(String[] args) throws Exception {
		String SuperHeros[] = {"Superman", "Batman", "Wonderwoman", "ElastiGirl", "Supergirl", "Aquagirl", "DreamGirl", "DynaGirl", "SpiderMan", "AntMan", "Thor", "HalJordan", "CaptainAmerica",
				"MartianManhunter", "DickGrayson", "Thing", "HumanTorch", "MrFantastic", "InvisibleWoman", "Superboy"};
		String dir1 = "Shahram";
		String TinyFileName = "Superbs";
		
		System.out.println(TestName + "Verify the chunk size is set to 1 MB, i.e., 1024*1024.");
		
		
		ClientFS cfs = new ClientFS();
		FSReturnVals fsrv = cfs.CreateDir("/", dir1);
		if ( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 6 result: fail!");
    		return;
		}
		int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		ClientRec crec = new ClientRec();
		
		System.out.println(TestName + "Create two files for superheroes: One for the name and the other for their images.");
		//Create two TinyFS filenames, one for the images and a second for the names
		fsrv = cfs.CreateFile("/" + dir1 + "/", TinyFileName + ".img");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 6 result: fail!");
    		return;
		}
		fsrv = cfs.CreateFile("/" + dir1 + "/", TinyFileName + ".names");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 6 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "Append superhero image and name records to the respective file.");
		//Open the two files
		FileHandle ImageFH = new FileHandle();
		FileHandle NameFH = new FileHandle();
		FSReturnVals imgofd = cfs.OpenFile("/" + dir1 + "/" + TinyFileName + ".img", ImageFH);
		FSReturnVals nameofd = cfs.OpenFile("/" + dir1 + "/" + TinyFileName + ".names", NameFH);
		
		for(int i = 0; i < SuperHeros.length; i++){
			String filename = SuperHeros[i];  //This is the file in the local directory
			
			byte[] indexBytes = ByteBuffer.allocate(intSize).putInt(i).array();
			long size = 0;
			byte[] contentBytes = getBytesFromFile(new File("SuperHeros/" + filename + ".jpg"), size);
			byte[] sizeBytes = ByteBuffer.allocate(intSize).putInt((int)size).array();
			byte[] IMG_rec = new byte[indexBytes.length + sizeBytes.length + contentBytes.length];
			
			//Construct the image record of the superhero
			System.arraycopy(indexBytes, 0, IMG_rec, 0, indexBytes.length);
			System.arraycopy(sizeBytes, 0, IMG_rec, indexBytes.length, sizeBytes.length);
			System.arraycopy(contentBytes, 0, IMG_rec, indexBytes.length + sizeBytes.length, contentBytes.length);
			
			RID rid = new RID();
			crec.AppendRecord(ImageFH, IMG_rec, rid);  //append the image record
			
			//construct the record containing the name of the superhero
			contentBytes = filename.getBytes();
			size = sizeBytes.length;
			sizeBytes = ByteBuffer.allocate(intSize).putInt(sizeBytes.length).array();
			byte[] NAME_rec = new byte[indexBytes.length + sizeBytes.length + contentBytes.length];
			
			System.arraycopy(indexBytes, 0, NAME_rec, 0, indexBytes.length);
			System.arraycopy(sizeBytes, 0, NAME_rec, indexBytes.length, sizeBytes.length);
			System.arraycopy(contentBytes, 0, NAME_rec, indexBytes.length + sizeBytes.length, contentBytes.length);
			
			rid = new RID();
			crec.AppendRecord(NameFH, NAME_rec, rid);

		}
		fsrv = cfs.CloseFile(ImageFH);
		if (fsrv != FSReturnVals.Success){
			System.out.println("Error in UnitTest6: Failed to close the Superhero image file");
			return;
		}
		
		fsrv = cfs.CloseFile(NameFH);
		if (fsrv != FSReturnVals.Success){
			System.out.println("Error in UnitTest6: Failed to close the Superhero image file");
			return;
		}

		System.out.println(TestName + "Open the TinyFS files and read the records to verify their accurracy.");
		//Check that the information was inserted correctly
		//Open the image and name files
		//Read the records and compare the payloads
		imgofd = cfs.OpenFile("/" + dir1 + "/" + TinyFileName + ".img", ImageFH);
		nameofd = cfs.OpenFile("/" + dir1 + "/" + TinyFileName + ".names", NameFH);
		byte[] imagePL = null, namePL = null;
		TinyRec img1 = new TinyRec();
		FSReturnVals retImg1 = crec.ReadFirstRecord(ImageFH, img1);
		TinyRec name1 = new TinyRec();
		FSReturnVals retName1 = crec.ReadFirstRecord(NameFH, name1);
		if(img1.getRID() == null || name1.getRID() == null){
			System.out.println("Error in UnitTest6:  Failed to read the first record");
			return;
		}
		for(int i = 0; i < SuperHeros.length; i++){
			String filename = SuperHeros[i];  //This is the file in the local directory
			if(i != 0){
				TinyRec img2 = new TinyRec();
				FSReturnVals retval1 = crec.ReadNextRecord(ImageFH, img1.getRID(), img2);
				if(img2.getRID() == null){
					System.out.println("Error in UnitTest6:  Failed to read the next record");
					return;
				}
				img1 = img2;
				TinyRec name2 = new TinyRec();
				FSReturnVals retval2 = crec.ReadNextRecord(NameFH, name1.getRID(), name2);
				if(name2.getRID() == null){
					System.out.println("Error in UnitTest6:  Failed to read the next record");
					return;
				}
				name1 = name2;
			}
			byte[] indexBytes = ByteBuffer.allocate(intSize).putInt(i).array();
			long size = 0;
			byte[] contentBytes = getBytesFromFile(new File("SuperHeros/" + filename + ".jpg"), size);
			byte[] sizeBytes = ByteBuffer.allocate(intSize).putInt((int)size).array();
			imagePL = img1.getPayload();
			for(int j = 0; j < imagePL.length; j++){
				if(j < 4){
					if(imagePL[j] != indexBytes[j]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}else if(j < 8){
					if(imagePL[j] != sizeBytes[j - 4]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}else{
					if(imagePL[j] != contentBytes[j - 8]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}
			}
			
			contentBytes = filename.getBytes();
			size = sizeBytes.length;
			sizeBytes = ByteBuffer.allocate(intSize).putInt(sizeBytes.length).array();
			namePL = name1.getPayload();
			for(int j = 0; j < namePL.length; j++){
				if(j < 4){
					if(namePL[j] != indexBytes[j]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}else if(j < 8){
					if(namePL[j] != sizeBytes[j - 4]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}else{
					if(namePL[j] != contentBytes[j - 8]){
						System.out.println("Unit test 6 result: fail!");
						return;
					}
				}
			}
		}
		fsrv = cfs.CloseFile(ImageFH);
		if (fsrv != FSReturnVals.Success){
			System.out.println("Error in UnitTest6:  Failed to close the Superhero image file");
			return;
		}
		
		fsrv = cfs.CloseFile(NameFH);
		if (fsrv != FSReturnVals.Success){
			System.out.println("Error in UnitTest6:  Failed to close the Superhero image file");
			return;
		}
		System.out.println(TestName + "Success!");
	}
	
	public static byte[] getBytesFromFile(File file, long length) throws IOException {
	       InputStream is = new FileInputStream(file);
	    
	       length = file.length();
	       if (length > Integer.MAX_VALUE) {
	           System.out.println("File is too large");
	       }
	    
	       // Create the byte array to hold the data
	       byte[] bytes = new byte[(int)length];
	    
	       // Read in the bytes
	       int offset = 0;
	       int numRead = 0;
	       while (offset < bytes.length
	              && (numRead=is.read(bytes, offset, bytes.length - offset)) >= 0) {
	           offset += numRead;
	       }
	       // Ensure all the bytes have been read in
	       if (offset < bytes.length) {
	           throw new IOException("Could not completely read file " + file.getName());
	       }
	       // Close the input stream and return bytes
	       is.close();
	       return bytes;
	   }

}
