package org.apache.hadoop.checker;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
//import org.apache.hadoop.dfs.protocol.FSConstants.DatanodeReportType;
//import org.apache.hadoop.mapred.ClusterStatus;
//import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.VersionInfo;
//import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
//import org.apache.hadoop.hdfs.protocol.ClientProtocol;

/********************************************************
 * MetadataReader can connect to a Hadoop Filesystem and 
 * perform basic file tasks.  It uses the ClientProtocol
 * to communicate with a NameNode daemon, and connects 
 * directly to DataNodes to read/write block data.
 *
 * Hadoop DFS users should obtain an instance of 
 * DistributedFileSystem, which uses DFSClient to handle
 * filesystem tasks.
 *
 ********************************************************/


public class MetadataReader {

	public static final Log LOG = LogFactory.getLog(MetadataReader.class);
	private static DFSClient client;
	//final ClientProtocol namenode;
    private enum NNStats {

        STATS_CAPACITY_IDX(0, 
                "Total storage capacity of the system, in bytes: ");
        //... see org.apache.hadoop.hdfs.protocol.ClientProtocol 

        private int id;
        private String desc;

        private NNStats(int id, String desc) {
            this.id = id;
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }

        public int getId() {
            return id;
        }

    }
/*
    private enum ClusterStats {
        //see org.apache.hadoop.mapred.ClusterStatus API docs
        USED_MEM {
            @Override
            public String getDesc() {
                String desc = "Total heap memory used by the JobTracker: ";
                return desc + clusterStatus.getUsedMemory();
            }
        };

        private static ClusterStatus clusterStatus;
        public static void setClusterStatus(ClusterStatus stat) {
            clusterStatus = stat;
        }

        public abstract String getDesc();
    }
*/

    public static void main(String[] args) throws Exception {

        //InetSocketAddress namenodeAddr = new InetSocketAddress("128.210.139.187",9000);
    	InetSocketAddress namenodeAddr = new InetSocketAddress("localhost",9000);
        Configuration conf = new Configuration();
        
        client = new DFSClient(namenodeAddr, conf);
        ClientProtocol namenode = client.getNamenode();
        long[] stats = namenode.getStats();
        LOG.info("NameNode Info: ");
        //System.out.println("NameNode Info: ");
        for (NNStats sf : NNStats.values()) {
            System.out.println(sf.getDesc() + stats[sf.getId()]); //Get total storage capacity of the system
        }

        //query JobTracker
        //InetSocketAddress jobtrackerAddr = new InetSocketAddress("localhost",9001);
        //JobClient jobClient = new JobClient(jobtrackerAddr, conf); 
        //ClusterStatus clusterStatus = jobClient.getClusterStatus(true);
        //System.out.println("\nJobTracker info: ");
        //System.out.println("State: " + clusterStatus.getJobTrackerState().toString());
        //ClusterStats.setClusterStatus(clusterStatus);
        //for (ClusterStats cs : ClusterStats.values()) {
        //   System.out.println(cs.getDesc());
        //}

        //Hadoop build version
        System.out.println("\nHadoop build version: "  + VersionInfo.getBuildVersion());
        //LOG.info("\nHadoop build version: [{}] ", VersionInfo.getBuildVersion());
        //Datanodes Information
        //System.out.println("\nDataNode Info: ");
        LOG.info("Datanode Info: ");
        DatanodeInfo[] datanodeReport = namenode.getDatanodeReport(DatanodeReportType.ALL); 
        // Return live datanodes if type is LIVE; dead datanodes if type is DEAD; otherwise all datanodes if type is ALL.
        for (DatanodeInfo di : datanodeReport) {
            System.out.println("Host: " + di.getHostName());
            System.out.println(di.getDatanodeReport());
        }

        // Get Metadata information from namenode
        
        
    }
    

}
