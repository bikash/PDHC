package org.apache.hadoop.checker;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.util.Time.now;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BackupImage;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.Checkpointer;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.CheckpointCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.Daemon;

import com.google.common.collect.Lists;

/**
 * The CheckNode is responsible for supporting periodic check up 
 * of the HDFS metadata in namenode.
 *
 * 
 * The start of a checkpoint is triggered by one of the two factors:
 * (1) time or (2) the size of the edits file.
 */
@SuppressWarnings("unused")
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

	private static NameNode checkerNode;

	private static DFSClient dfsClient;
	
	private NameNode namenode;


      


    /**
     * Create a connection to the primary namenode.
     */
	CheckerNode(Configuration conf, BackupNode bnNode)  throws IOException {
      this.conf = conf;
      this.backupNode = bnNode;
      try {
        initialize(conf);
      } catch(IOException e) {
        LOG.warn("Checkpointer got exception", e);
        shutdown();
        throw e;
      }
    }
    
	/**
	   * Initialize checkpoint.
	   */
	  private void initialize(Configuration conf) throws IOException {
	    // Create connection to the namenode.
	    shouldRun = true;

	    // Initialize other scheduling parameters from the configuration
	    checkpointConf = new CheckpointConf(conf);

	    // Pull out exact http address for posting url to avoid ip aliasing issues
	    //String fullInfoAddr = conf.get(DFS_NAMENODE_BACKUP_HTTP_ADDRESS_KEY, 
	                                 //  DFS_NAMENODE_BACKUP_HTTP_ADDRESS_DEFAULT);
	    String fullInfoAddr = conf.get("127.0.0.1", "127.0.0.1");
	    infoBindAddress = fullInfoAddr.substring(0, fullInfoAddr.indexOf(":"));

	    LOG.info("Checkpoint Period : " +
	             checkpointConf.getPeriod() + " secs " +
	             "(" + checkpointConf.getPeriod()/60 + " min)");
	    LOG.info("Transactions count is  : " +
	             checkpointConf.getTxnCount() +
	             ", to trigger checkpoint");
	  }

	  /**
	   * Shut down the checkpointer.
	   */
	  void shutdown() {
	    shouldRun = false;
	    backupNode.stop();
	  }
	  
	  //
	  // The main work loop
	  //
	  public void run() {
	    // Check the size of the edit log once every 5 minutes.
	    long periodMSec = 5 * 60;   // 5 minutes
	    if(checkpointConf.getPeriod() < periodMSec) {
	      periodMSec = checkpointConf.getPeriod();
	    }
	    periodMSec *= 1000;

	    long lastCheckpointTime = 0;
	    if (!backupNode.shouldCheckpointAtStartup()) {
	      lastCheckpointTime = now();
	    }
	    while(shouldRun) {
	      try {
	        long now = now();
	        boolean shouldCheckpoint = false;
	        if(now >= lastCheckpointTime + periodMSec) {
	          shouldCheckpoint = true;
	        } else {
	          long txns = countUncheckpointedTxns();
	          if(txns >= checkpointConf.getTxnCount())
	            shouldCheckpoint = true;
	        }
	        if(shouldCheckpoint) {
	          doCheckpoint();
	          lastCheckpointTime = now;
	        }
	      } catch(IOException e) {
	        LOG.error("Exception in doCheckpoint: ", e);
	      } catch(Throwable e) {
	        LOG.error("Throwable Exception in doCheckpoint: ", e);
	        shutdown();
	        break;
	      }
	      try {
	        Thread.sleep(periodMSec);
	      } catch(InterruptedException ie) {
	        // do nothing
	      }
	    }
	  }

	  private long countUncheckpointedTxns() throws IOException {
		    long curTxId = getRemoteNamenodeProxy().getTransactionID();
		    long uncheckpointedTxns = curTxId -
		      getFSImage().getStorage().getMostRecentCheckpointTxId();
		    assert uncheckpointedTxns >= 0;
		    return uncheckpointedTxns;
		  }

		  /**
		   * Create a new checkpoint
		   */
	 public void doCheckpoint() throws IOException {
		    BackupImage bnImage = getFSImage();
		    NNStorage bnStorage = bnImage.getStorage();

		    long startTime = now();
		    bnImage.freezeNamespaceAtNextRoll();
		    
		    NamenodeCommand cmd = 
		      getRemoteNamenodeProxy().startCheckpoint(backupNode.getRegistration());
		    CheckpointCommand cpCmd = null;
		    switch(cmd.getAction()) {
		      case NamenodeProtocol.ACT_SHUTDOWN:
		        shutdown();
		        throw new IOException("Name-node " + backupNode.nnRpcAddress
		                                           + " requested shutdown.");
		      case NamenodeProtocol.ACT_CHECKPOINT:
		        cpCmd = (CheckpointCommand)cmd;
		        break;
		      default:
		        throw new IOException("Unsupported NamenodeCommand: "+cmd.getAction());
		    }

		    bnImage.waitUntilNamespaceFrozen();
		    
		    CheckpointSignature sig = cpCmd.getSignature();

		    // Make sure we're talking to the same NN!
		    sig.validateStorageInfo(bnImage);

		    long lastApplied = bnImage.getLastAppliedTxId();
		    LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
		    RemoteEditLogManifest manifest =
		      getRemoteNamenodeProxy().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);

		    boolean needReloadImage = false;
		    if (!manifest.getLogs().isEmpty()) {
		      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);
		      // we don't have enough logs to roll forward using only logs. Need
		      // to download and load the image.
		      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
		        LOG.info("Unable to roll forward using only logs. Downloading " +
		            "image with txid " + sig.mostRecentCheckpointTxId);
		        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
		            backupNode.nnHttpAddress, sig.mostRecentCheckpointTxId, bnStorage,
		            true);
		        bnImage.saveDigestAndRenameCheckpointImage(NameNodeFile.IMAGE,
		            sig.mostRecentCheckpointTxId, downloadedHash);
		        lastApplied = sig.mostRecentCheckpointTxId;
		        needReloadImage = true;
		      }

		      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
		        throw new IOException("No logs to roll forward from " + lastApplied);
		      }
		  
		      // get edits files
		      for (RemoteEditLog log : manifest.getLogs()) {
		        TransferFsImage.downloadEditsToStorage(
		            backupNode.nnHttpAddress, log, bnStorage);
		      }

		      if(needReloadImage) {
		        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
		        File file = bnStorage.findImageFile(NameNodeFile.IMAGE,
		            sig.mostRecentCheckpointTxId);
		        bnImage.reloadFromImageFile(file, backupNode.getNamesystem());
		      }
		      rollForwardByApplyingLogs(manifest, bnImage, backupNode.getNamesystem());
		    }
		    
		    long txid = bnImage.getLastAppliedTxId();
		    
		    backupNode.namesystem.writeLock();
		    try {
		      backupNode.namesystem.setImageLoaded();
		      if(backupNode.namesystem.getBlocksTotal() > 0) {
		        backupNode.namesystem.setBlockTotal();
		      }
		      bnImage.saveFSImageInAllDirs(backupNode.getNamesystem(), txid);
		      bnStorage.writeAll();
		    } finally {
		      backupNode.namesystem.writeUnlock();
		    }

		    if(cpCmd.needToReturnImage()) {
		      TransferFsImage.uploadImageFromStorage(backupNode.nnHttpAddress, conf,
		          bnStorage, NameNodeFile.IMAGE, txid);
		    }

		    getRemoteNamenodeProxy().endCheckpoint(backupNode.getRegistration(), sig);

		    if (backupNode.getRole() == NamenodeRole.BACKUP) {
		      bnImage.convergeJournalSpool();
		    }
		    backupNode.setRegistration(); // keep registration up to date
		    
		    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
		    LOG.info("Checkpoint completed in "
		        + (now() - startTime)/1000 + " seconds."
		        + " New Image Size: " + imageSize);
		  }
  
	 private URL getImageListenAddress() {
		    InetSocketAddress httpSocAddr = backupNode.getHttpAddress();
		    int httpPort = httpSocAddr.getPort();
		    try {
		      return new URL(DFSUtil.getHttpClientScheme(conf) + "://" + infoBindAddress + ":" + httpPort);
		    } catch (MalformedURLException e) {
		      // Unreachable
		      throw new RuntimeException(e);
		    }
		  }

    public static void rollForwardByApplyingLogs(
		      RemoteEditLogManifest manifest,
		      FSImage dstImage,
		      FSNamesystem dstNamesystem) throws IOException {
		    NNStorage dstStorage = dstImage.getStorage();
		  
		    List<EditLogInputStream> editsStreams = Lists.newArrayList();    
		    for (RemoteEditLog log : manifest.getLogs()) {
		      if (log.getEndTxId() > dstImage.getLastAppliedTxId()) {
		        File f = dstStorage.findFinalizedEditsFile(
		            log.getStartTxId(), log.getEndTxId());
		        editsStreams.add(new EditLogFileInputStream(f, log.getStartTxId(), 
		                                                    log.getEndTxId(), true));
		      }
		    }
		    LOG.info("Checkpointer about to load edits from " +
		        editsStreams.size() + " stream(s).");
		    dstImage.loadEdits(editsStreams, dstNamesystem);
		  }

	 
	 
	 
	 
	  
	  private static NamenodeProtocol getRemoteNamenodeProxy(){
		    return checkerNode.namenode;
		  }	
	  
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
	  private void getMetadataNamenode(){
		  
	  }
	  /**
	   * Return datanode information.
	   * @return DatanodeInfo
	   */
	  private static void getDataNodeBlockSummary()  throws IOException {
		  // Make sure we're talking to the same NN!
		    //sig.validateStorageInfo(bnImage);

		    //long lastApplied = bnImage.getLastAppliedTxId();
		   // LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
		  
	  }
	  private static BackupImage getFSImage() {
		    return (BackupImage)checkerNode.getFSImage();
		  }
	  
	  
	  // get the block location for particular file in hdfs
	  public void getBlockLocations(String source, Configuration conf) throws IOException{

		  FileSystem fileSystem = FileSystem.get(conf);
		  Path srcPath = new Path(source);
		   
		  // Check if the file already exists
		  /*if (!(ifExists(srcPath))) {
			  System.out.println("No such destination " + srcPath);
			  return;
		  }*/
		  // Get the filename out of the file path
		  String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		   
		  FileStatus fileStatus = fileSystem.getFileStatus(srcPath);   
		  BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		  int blkCount = blkLocations.length;
		   
		  System.out.println("File :" + filename + "stored at:");
		  for (int i=0; i < blkCount; i++) {
		  String[] hosts = blkLocations[i].getHosts();
		  System.out.format("Host %d: %s %n", i, hosts);
		  }
		   
		  }


      
      
	public static void main(String[] args) throws Exception {
		  //Configuration conf = new HdfsConfiguration();
		  Configuration conf = new Configuration();
		  conf.addResource(new Path("/Users/bikash/BigData/hadoop/etc/hadoop/core-site.xml"));
		  conf.addResource(new Path("/Users/bikash/BigData/hadoop/etc/hadoop/hdfs-site.xml"));
		  conf.addResource(new Path("/Users/bikash/BigData/hadoop/etc/hadoop/mapred-site.xml"));
		  //getDatanodeReport(conf);
		  //MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
		  //getDataNodeSummaryReport(conf,cluster);
		  
		  //DataNode dataNode = cluster.getDataNodes().get(0);
		  //int infoPort = dataNode.getInfoPort();
		  //System.out.println("Infoport "+infoPort);
		  
		 // final String blockpoolID = getBlockPoolID(conf);
		 // LOG.info("BlockPoolId is " + blockpoolID);
		  
	  }

	// get the datanode summary report
	public static void getDatanodeReport( Configuration conf ) throws IOException{
		conf.setInt( DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
		conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
		MiniDFSCluster cluster = 
			      new MiniDFSCluster.Builder(conf).numDataNodes(4).build();
		 final String bpid = cluster.getNamesystem().getBlockPoolId();
		 final List<DataNode> datanodes = cluster.getDataNodes();
	     final DFSClient client = cluster.getFileSystem().dfs;
	}
	
    private static String getBlockPoolID(Configuration conf) throws IOException {

        final Collection<URI> namenodeURIs = DFSUtil.getNsServiceRpcUris(conf);
        URI nameNodeUri = namenodeURIs.iterator().next();

        final NamenodeProtocol namenode = NameNodeProxies.createProxy(conf, nameNodeUri, NamenodeProtocol.class)
            .getProxy();
        final NamespaceInfo namespaceinfo = namenode.versionRequest();
        return namespaceinfo.getBlockPoolID();
    }  
	  

	  
	  
}
