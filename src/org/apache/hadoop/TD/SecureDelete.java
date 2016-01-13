package org.apache.hadoop.TD;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import estreamj.ciphers.trivium.Trivium;
import estreamj.framework.ESJException;

/**
 * Secure file delete.
 * 
 * @author Bikash
 *
 */
public class SecureDelete {
        private static final String TAG = "SecureDelete";
        private static final Logger LOGGER = LoggerFactory.getLogger(SecureDelete.class);
        static final long seed = 0xDEADBEEFL; 
        
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
        /**
         * Securely delete a file.
         * 
         * Currently, there is only 1 pass that overwrites the file first
         * with a random bit stream generated by Trivium.
         * 
         * @param file
         * @return true if this File was deleted, false otherwise.
         */
        public static boolean delete(File file) {

    
        	
        	
        	if (file.exists()) {
                        SecureRandom random = new SecureRandom();

                        Trivium tri = new Trivium();
                        
                        try {
                                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                                FileChannel channel = raf.getChannel();
                                MappedByteBuffer buffer = channel.map(
                                                FileChannel.MapMode.READ_WRITE, 0, raf.length());

                                byte[] key = new byte[10];
                                byte[] nonce = new byte[10];
                                random.nextBytes(key);
                                random.nextBytes(nonce);
                                
                                tri.setupKey(Trivium.MODE_DECRYPT,
                                                key, 0);
                                tri.setupNonce(nonce, 0);

                                int buffersize = 1024;
                                byte[] bytes = new byte[1024];
                                
                                // overwrite with random numbers
                                while (buffer.hasRemaining()) {
                                        int max = buffer.limit() - buffer.position();
                                        if (max > buffersize) max = buffersize;
                                        //random.nextBytes(bytes);

                                        tri.process(bytes, 0,
                                                        bytes, 0, max);

                                        buffer.put(bytes, 0, max);
                                }
                                buffer.force();
                                buffer.rewind();

                        } catch (FileNotFoundException e) {
                        	LOGGER.info(TAG, "FileNotFoundException", e);
                        } catch (IOException e) {
                        	LOGGER.info(TAG, "IOException", e);
                        } catch (ESJException e) {
                        	LOGGER.info(TAG, "ESJException", e);
                        }
                        return file.delete();
                }
                return false;
        }
        
        /***
         * Get the block location
         * 
         */
        
        public static void getBlockLocations(String source, Configuration conf ) throws IOException{
    			
    		FileSystem fileSystem = FileSystem.get(conf);
    		Path srcPath = new Path(source);

    		// Check if the file already exists
           /* if (!(ifExists(srcPath))) {
                System.out.println("No such destination " + srcPath);
                return;
            }*/
            // Get the filename out of the file path
            String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
            
            FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
          
            System.out.println("File :" + fileStatus.toString() );
            BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            int blkCount = blkLocations.length;
            System.out.println("Length "+ blkLocations[0].toString() + " blk count "+ blkCount);
            System.out.println("File :" + filename);
            for (int i=0; i < blkCount; i++) {
            
              String[] hosts = blkLocations[i].getHosts();
              System.out.format("Host %s: %s %n", hosts[i], blkLocations[0].toString());
            }
            String[ ] names = blkLocations[0].getNames();
            int namesCount = names.length;
            for (int j=0;  j < namesCount; j++){
                    System.out.println(names[j]);
            }
            String[ ] topology = blkLocations[0].getTopologyPaths();
            int topCount = topology.length;
            for (int j=0; j < topCount; j++){
                    System.out.println(topology[j]);
            }

            System.out.println("Offset: " + blkLocations[0].getOffset());
            System.out.println("Length: " + blkLocations[0].getLength());

    	}
        
        /**
         * Calculate the Number of blocks and rewrite the blocks
         */
        private static void DeleteFile(String source, Configuration conf) throws IOException { 
        	
        	FileSystem fileSys = FileSystem.get(conf);
    		Path srcPath = new Path(source);
    		
        	// Get the filename out of the file path
           // String filename = source.substring(source.lastIndexOf('/') + 1, source.length());            
            FileStatus fileStatus = fileSys.getFileStatus(srcPath);
            long size = fileStatus.getLen();
            System.out.println("File :" + fileStatus.toString() );
            BlockLocation[] locations = fileSys.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

            System.out.println("Number of blocks" + locations.toString() +" Leng "+ locations.length); 
            FSDataInputStream stm = fileSys.open(srcPath); 
            stm.seek(0);
            //FSDataOutputStream ostm = fileSys.create(srcPath, true); 
            byte[] expected = new byte[(int) fileStatus.getLen()]; 
            Random rand = new Random(); 
            String content ="";
            for(int i=0; i<size; i++)
            {
            	int randomNum = rand.nextInt((1 - 0) + 1) + 0;
            	content = content + randomNum;
            }
           
            FSDataOutputStream fsout = fileSys.create(srcPath);
               // wrap the outputstream with a writer
            PrintWriter writer = new PrintWriter(fsout);
            writer.append(content);
            writer.close();

            stm.close(); 
            //stm.write(content); 
            //ostm.writeUTF(content);
            //rand.nextBytes(expected); 
            //ostm.write((byte)content); 
            // do a sanity check. Read the file 
            //byte[] actual = new byte[(int) fileStatus.getLen()]; 
            //stm.readFully(0, actual); 
            //checkAndOverWriteData(actual, 0, expected, "Read Sanity Test"); 
           // stm.close(); 
            //ostm.close();
          } 
        /**
         * Overwrite the file
         * 
         * 
         * 
         */
        private static void checkAndOverWriteData(byte[] actual, int from, byte[] expected, String message) { 
            for (int idx = 0; idx < actual.length; idx++) { 
            	System.out.format(message+" byte "+(from+idx)+" differs. expected "+ 
                                expected[from+idx]+" actual "+actual[idx], 
                                actual[idx], expected[from+idx]); 
              actual[idx] = 0; 
            } 
          } 
           
        /**
         * Securely delete a file.
         * 
         * Currently, there is only multi pass that overwrites the file first
         * with a random bit stream generated by Trivium.
         * 
         * @param file, pass
         * @return true if this File was deleted, false otherwise.
         * @throws IOException 
         */
        public static boolean delete(String file, int pass, Configuration conf) throws IOException {
        	
        	FileSystem fs = FileSystem.get(conf);
       
        	//return true;
        	File file1 = new File(file);
        	if (fs.exists(new Path(file))) {
        		System.out.println("File " + file1 + " does exists");
        		//getBlockLocations(file,conf);
        		for(int i=0;i<pass;i++){        
        				//System.out.println("File " + i);
        				DeleteFile(file,conf);
        				LOGGER.info(TAG, "Number of Pass ", i);
                                     
                }
        		//return file1.delete();
        	}
            return false;
        }
}