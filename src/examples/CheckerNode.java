package examples;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.util.Time.now;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BackupImage;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;

import com.google.common.collect.Lists;

/**
 * The CheckNode is responsible for supporting periodic check up 
 * of the HDFS metadata in namenode.
 *
 * The Checker is a daemon that periodically wakes up
 * up (determined by the schedule specified in the configuration),
 * triggers a periodic checkpoint and then goes back to sleep.
 * 
 * The start of a checkpoint is triggered by one of the two factors:
 * (1) time or (2) the size of the edits file.
 */
@SuppressWarnings("unused")
public class CheckerNode {

	 public static final Log LOG = LogFactory.getLog(CheckerNode.class.getName());
	 
	 //private final BackupNode backupNode;


	  private String infoBindAddress;

	  private CheckpointConf checkpointConf;
	  //private final static Configuration conf;

	  private Path workingDir;
	  private URI uri;

	  DFSClient dfs;

	private static DFSClient dfsClient;
	  
	  /*private BackupImage getFSImage() {
	    NameNode backupNode = null;
		return (BackupImage)backupNode.getFSImage();
	  }*/

	 /* private NamenodeProtocol getRemoteNamenodeProxy(){
	    return backupNode.namenode;
	  }*/
	  

	  /*public Path getHomeDirectory() {
	    return makeQualified(new Path("/user/" + dfs.ugi.getShortUserName()));
	  }*/

		
	  
	  /**
	   * Return datanode information.
	   * @return DatanodeInfo
	   */
	  private static DatanodeInfo getDataNodeSummaryReport(Configuration conf, MiniDFSCluster cluster)  throws IOException {
	    dfsClient = new DFSClient(new InetSocketAddress("localhost", 
                cluster.getNameNodePort()), conf);
		DatanodeInfo dn = dfsClient.datanodeReport(DatanodeReportType.LIVE)[0];
		return dn;
	  }
	  
	  /**
	   * Return datanode information.
	   * @return DatanodeInfo
	   */
	  private static void getDataNodeBlockSummary()  throws IOException {
		  
		  
	  }
	  public static void main(String[] args) throws Exception {
		  Configuration conf = new HdfsConfiguration();
		  MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
		  //getDataNodeSummaryReport(conf,cluster);
		  
		  DataNode dataNode = cluster.getDataNodes().get(0);
		  int infoPort = dataNode.getInfoPort();
		  System.out.println("Infoport "+infoPort);
	  }

	  
	  

	  
	  
}
