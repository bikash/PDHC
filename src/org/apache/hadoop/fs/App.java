package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try{
            
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://128.210.139.187:9000/");
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path("hdfs://128.210.139.187:9000/"));  // you need to pass in your hdfs path

            for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    line=br.readLine();
                }
            }
        }catch(Exception e){
            System.out.println("File not found");
        }
	}

}
