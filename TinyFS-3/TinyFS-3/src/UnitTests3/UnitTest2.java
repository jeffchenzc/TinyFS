package UnitTests3;

import java.util.ArrayList;
import java.util.Arrays;

import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;

/**
 * UnitTest2 for Part 3 of TinyFS
 * @author Shahram Ghandeharizadeh and Jason Gui
 *
 */
public class UnitTest2 {
	
	static final String TestName = "Unit Test 2: ";

	public static void main(String[] args) {
		test2(new ClientFS());
	}
	
	public static void test2(ClientFS cfs){
		UnitTest1 ut1 = new UnitTest1();
		ut1.test1(cfs);
		int N = ut1.N;
		
		System.out.println(TestName + "DeleteDir(\"/Shahram/N\"), ListDir(\"/Shahram\") and verify direcotry N is gone (success)");
		String dir1 = "Shahram";
		FSReturnVals fsrv = cfs.DeleteDir("/" + dir1 + "/", String.valueOf(N));
		String[] ret1 = cfs.ListDir("/" + dir1);
		
		boolean isExist = isDirExist(ret1, "/" + dir1+"/"+N);
		if(isExist == true){
			System.out.println("Unit test 2 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "DeleteDir(\"/Ghandeharizadeh/1/2/.../N\"), ListDir(\"/Ghandeharizadeh/1/2/.../\") and verify directory N is gone (success)");
		String dir2 = "Ghandeharizadeh";
		String lastSec = "/" + dir2;
		for(int i = 1; i < N; i++){
			lastSec = lastSec + "/" + i;
		}
		fsrv = cfs.DeleteDir(lastSec + "/", String.valueOf(N));
		
		String[] ret2 = cfs.ListDir(lastSec);
		isExist = isDirExist(ret2, lastSec + "/" + N);
		if(isExist == true){
			System.out.println("Unit test 2 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "DeleteDir(\"/Shahram\") and verify it return the correct failure code");
		fsrv = cfs.DeleteDir("/", dir1);
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good! Detected " + dir1 + " exists.");
		} else {
			System.out.println("Unit test 2 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "DeleteDir(\"/Ghandeharizadeh/1/2\") and verifty it returns the correct failure code");
		fsrv = cfs.DeleteDir("/" + dir2 + "/1/", "2");
		if(fsrv == FSReturnVals.DirNotEmpty){
			System.out.println("Good!  Detected /" + dir2 + "/1/2 exists.");
		} else {
			System.out.println("Unit test 2 result: fail!");
    		return;
		}
		
		System.out.println(TestName + "RenameDir(\"/Shahram/i\", \"/Shahram/1i\") for i from 1 to N-1.  ListDir(\"/Shahram\") and verify the N-1 returns dirs are 1i to (N-1)i");
		for(int i = 1; i < N; i++){
			fsrv = cfs.RenameDir("/" + dir1 + "/" + i, "/" + dir1 + "/" + i + "i");
			if( fsrv != FSReturnVals.Success){
				System.out.println("Unit test 2 result: fail!");
	    		return;
			}
		}
		
		System.out.println(TestName + "RenameDir(\"/Ghandeharizadeh\", \"/ShahramGhandeharizadeh\")");
		fsrv = cfs.RenameDir("/" + dir2, "/ShahramGhandeharizadeh");
		if( fsrv != FSReturnVals.Success ){
			System.out.println("Unit test 2 result: fail!");
    		return;
		}
		System.out.println(TestName + "Success!");
	}
	
	public static boolean isDirExist(String[] arr, String token){
		if (arr == null || arr.length == 0) {
			return false;
		}
		for (int i=0; i < arr.length; i++)
			if (arr[i].equals(token)) return true;
		return false;
	}

}
