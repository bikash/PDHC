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
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.namenode.BackupImage;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
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
public class CheckerNode {

	 public static final Log LOG = LogFactory.getLog(CheckerNode.class.getName());
	 
	 private final BackupNode backupNode;
	  volatile boolean shouldRun;

	  private String infoBindAddress;

	  private CheckpointConf checkpointConf;
	  private final Configuration conf;

	  private Path workingDir;
	  private URI uri;

	  DFSClient dfs;
	  
	  private BackupImage getFSImage() {
	    return (BackupImage)backupNode.getFSImage();
	  }

	  private NamenodeProtocol getRemoteNamenodeProxy(){
	    return backupNode.namenode;
	  }
	  

	  /*public Path getHomeDirectory() {
	    return makeQualified(new Path("/user/" + dfs.ugi.getShortUserName()));
	  }*/

	  /**
	   * Create a connection to the primary namenode.
	   */
	  CheckerNode(Configuration conf, BackupNode bnNode)  throws IOException {
	    this.conf = conf;
	    this.backupNode = bnNode;
	    
	    Statistics statistics = null;
		this.dfs = new DFSClient(uri, conf, statistics);
	    this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
	    //this.workingDir = getHomeDirectory();
	    try {
	      //initialize(conf);
	    } catch(IOException e) {
	      LOG.warn("Checkpointer got exception", e);
	      //shutdown();
	      throw e;
	    }
	  }
}
