package org.apache.hadoop.TD;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.swing.Spring;

import org.apache.hadoop.fs.Path;

public class SecureDelRandom {
	
	 /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see java.util.Random#nextInt(int)
     */
    public static int randInt(int min, int max) {
        Random rand = new Random();

        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        int randomNum = rand.nextInt((max - min) + 1) + min;

        return randomNum;
    }

	
	public static void secureDelete(File file) throws IOException {
		if (file.exists()) {
			long length = file.length();
			System.out.println("Length :" + length );
			SecureRandom random = new SecureRandom();
			
			RandomAccessFile raf = new RandomAccessFile(file, "rws");
			raf.seek(0);
			raf.getFilePointer();
			byte[] data = new byte[64];
			int pos = 0;
			Random rand = new Random(); 
			String content ="";
			for(int i=0; i<length; i++)
            {
            	int randomNum = rand.nextInt((1 - 0) + 1) + 0;
            	content = content + randomNum;            	
            }
			raf.writeUTF(content);
			raf.close();
			//file.delete();
		}
	}
	
	public static void main(String[] args) throws IOException {
		File directory = new File("/Users/bikash/hadoop/datanode/current/BP-1467327831-192.168.254.33-1446106217900/current/finalized/subdir0/subdir0");

		String [] directoryContents = directory.list();

		List<String> fileLocations = new ArrayList<String>();

		for(String fileName: directoryContents) {
		    File temp = new File(String.valueOf(directory),fileName);
		    System.out.println("Block Name" + String.valueOf(temp)); 
		    String file = String.valueOf(temp);
		    File f = new File(file);
		    secureDelete(f);
		    //fileLocations.add(String.valueOf(temp));
		}
		
		//String file = "/Users/bikash/hadoop/datanode/current/BP-1467327831-192.168.254.33-1446106217900/current/finalized/subdir0/subdir0/blk_1073741953";
		//Path path = new Path(file);
		//File f = new File(file);
        //SecureDelete.delete(file,7, conf);
		//secureDelete(f);
	 }
	
	
}
