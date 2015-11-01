package examples;


import java.net.InetSocketAddress;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.mapred.ClusterStatus;

public class metadataReader {

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


    public static void main(String[] args) throws Exception {

        InetSocketAddress namenodeAddr = new InetSocketAddress("myhost",8020);
        InetSocketAddress jobtrackerAddr = new InetSocketAddress("myhost",8021);

        Configuration conf = new Configuration();

        //query NameNode
        DFSClient client = new DFSClient(namenodeAddr, conf);
        ClientProtocol namenode = client.namenode;
        long[] stats = namenode.getStats();

        System.out.println("NameNode info: ");
        for (NNStats sf : NNStats.values()) {
            System.out.println(sf.getDesc() + stats[sf.getId()]);
        }

        //query JobTracker
        JobClient jobClient = new JobClient(jobtrackerAddr, conf); 
        ClusterStatus clusterStatus = jobClient.getClusterStatus(true);

        System.out.println("\nJobTracker info: ");
        System.out.println("State: " + 
                clusterStatus.getJobTrackerState().toString());

        ClusterStats.setClusterStatus(clusterStatus);
        for (ClusterStats cs : ClusterStats.values()) {
            System.out.println(cs.getDesc());
        }

        System.out.println("\nHadoop build version: " 
                + VersionInfo.getBuildVersion());

        //query Datanodes
        System.out.println("\nDataNode info: ");
        DatanodeInfo[] datanodeReport = namenode.getDatanodeReport(
                DatanodeReportType.ALL);
        for (DatanodeInfo di : datanodeReport) {
            System.out.println("Host: " + di.getHostName());
            System.out.println(di.getDatanodeReport());
        }

    }

}