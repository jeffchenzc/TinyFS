package UnitTests3;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
import com.client.ClientRec;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;

/**
 * UnitTest4 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest4 {
	
	public static int NumRecs = 1000;
	static final String TestName = "Unit Test 4: ";
	
	public static void main(String[] args) {
		String dir1 = "Shahram";
		ClientFS cfs = new ClientFS();
		FSReturnVals fsrv = cfs.CreateDir("/", dir1);
		if ( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 4 result: fail!");
    		return;
		}
		fsrv = cfs.CreateFile("/" + dir1 + "/", "emp");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 4 result: fail!");
    		return;
		}
		//get the file handle first
		FileHandle fh = new FileHandle();
		FSReturnVals ofd = cfs.OpenFile("/" + dir1 + "/emp", fh);
		byte[] payload = null;
		int intSize = Integer.SIZE / Byte.SIZE;	// 4 bytes
		ClientRec crec = new ClientRec();
		
		System.out.println(TestName + "Construct a record with the first four bytes equal to i, followed with 5 char attributes each with length 20.");
		for (int i = 0; i < NumRecs; i++){
			payload = new byte[104];
			byte[] ValInBytes = ByteBuffer.allocate(intSize).putInt(i).array();
			System.arraycopy(ValInBytes, 0, payload, 0, intSize);
			for(int j = 4; j < 104; j++){
				payload[j] = 'a';
			}
			RID rid = new RID();
			crec.AppendRecord(fh, payload, rid);
		}
		fsrv = cfs.CloseFile(fh);
		
		System.out.println(TestName + "Scan all records in a file");
		ofd = cfs.OpenFile("/" + dir1 + "/emp", fh);
		TinyRec r1 = new TinyRec();
		FSReturnVals retRR = crec.ReadFirstRecord(fh, r1);
		int cntr = 1;
		ArrayList<RID> vect = new ArrayList<RID>();
		while (r1.getRID() != null){
			TinyRec r2 = new TinyRec();
			FSReturnVals retval = crec.ReadNextRecord(fh, r1.getRID(), r2);
			//if(retval != FSReturnVals.Success){
			if(r2.getRID() != null){
				byte[] head = new byte[4];
				System.arraycopy(r2.getPayload(), 0, head, 0, 4);
				int value = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16)
				        | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
				
				//Store r2 in a vector
				if(value % 2 != 0){
					vect.add(r2.getRID());
				}
				r1 = r2;
				cntr++;
			} else {
				r1.setRID(null);
			}
				
		}
		
		System.out.println(TestName + "Delete the odd numbered records using their first four bytes.");
		//Iterate the vector and delete the RIDs stored in it
		for(int i = 0; i < vect.size(); i++){
			fsrv = crec.DeleteRecord(fh, vect.get(i));
			if(fsrv != FSReturnVals.Success){
				System.out.println("Unit test 4 result: failed to delete the record!");
				return;
			}
		}
		
		fsrv = cfs.CloseFile(fh);
		if(cntr != NumRecs){
			System.out.println("Unit test 4 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "Scan the file and verify there are only even numbered records using their first four bytes.");
		ofd = cfs.OpenFile("/" + dir1 + "/emp", fh);
		r1 = new TinyRec();
		retRR = crec.ReadFirstRecord(fh, r1);
		while (r1.getRID() != null){
			TinyRec r2 = new TinyRec();
			FSReturnVals retval = crec.ReadNextRecord(fh, r1.getRID(), r2);
			if(r2.getRID() != null){
				byte[] head = new byte[4];
				System.arraycopy(r2.getPayload(), 0, head, 0, 4);
				int value = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16)
				        | ((head[2] & 0xFF) << 8) | (head[3] & 0xFF);
				if(value % 2 != 0){
					System.out.println("Unit test 4 result: fail!  Found an odd numbered record with value " + value + ".");
		    		return;
				}
				r1 = r2;
			}else{
				r1.setRID(null);
			}
		}
		fsrv = cfs.CloseFile(fh);
		System.out.println(TestName + "Success!");
	}

}
