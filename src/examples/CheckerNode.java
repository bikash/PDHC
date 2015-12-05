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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.BackupImage;
import org.apache.hadoop.hdfs.server.namenode.BackupNode;
import org.apache.hadoop.hdfs.server.namenode.CheckpointConf;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
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

	private static NameNode checkerNode;

	private static DFSClient dfsClient;
	
	private NameNode namenode;

    public CheckerNode(NameNode nn){
   	   this.namenode=nn;
      }
      
      
	  
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
	  
	  
	  // get the bloack location for particlar file in hdfs
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


      // NamenodeProtocol interface
      /*public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
   		   throws IOException{
   	   return namenode.getBlocks(datanode, size);
      }
      
      public ExportedBlockKeys getBlockKeys() throws IOException{
   	   return namenode.getBlockKeys();
      }
      
      public long getEditLogSize() throws IOException{
   	   return namenode.getEditLogSize();
      }
      
      public CheckpointSignature rollEditLog() throws IOException{
   	   return namenode.rollEditLog();
      }
      
      public void rollFsImage() throws IOException{
   	   namenode.rollFsImage();
      }*/
      
      
	public static void main(String[] args) throws Exception {
		  //Configuration conf = new HdfsConfiguration();
		  Configuration conf = new Configuration();
		  //conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
		  //conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
		  //conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));
		  
		  //MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
		  //getDataNodeSummaryReport(conf,cluster);
		  
		  //DataNode dataNode = cluster.getDataNodes().get(0);
		  //int infoPort = dataNode.getInfoPort();
		  //System.out.println("Infoport "+infoPort);
		  
		  //BackupImage bnImage = getFSImage();
		 // NNStorage bnStorage = bnImage.getStorage();
		  //long startTime = now();
		  //bnImage.freezeNamespaceAtNextRoll();
		  //CheckpointCommand cpCmd = null;
		  //bnImage.waitUntilNamespaceFrozen();
		  //CheckpointSignature sig = cpCmd.getSignature();
		  // Make sure we're talking to the same NN!
		  //sig.validateStorageInfo(bnImage);
		  
		/*  long lastApplied = bnImage.getLastAppliedTxId();
		  LOG.debug("Doing checkpoint. Last applied: " + lastApplied);
		  RemoteEditLogManifest manifest = getRemoteNamenodeProxy().getEditLogManifest(bnImage.getLastAppliedTxId() + 1);
		  boolean needReloadImage = false;
		  if (!manifest.getLogs().isEmpty()) {
		      RemoteEditLog firstRemoteLog = manifest.getLogs().get(0);
		      // we don't have enough logs to roll forward using only logs. Need
		      // to download and load the image.
		      if (firstRemoteLog.getStartTxId() > lastApplied + 1) {
		        LOG.info("Unable to roll forward using only logs. Downloading " +"image with txid " );
		        MD5Hash downloadedHash = TransferFsImage.downloadImageToStorage(
		        		checkerNode.nnHttpAddress, sig.mostRecentCheckpointTxId, bnStorage,
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
		        		checkerNode.nnHttpAddress, log, bnStorage);
		      }

		      if(needReloadImage) {
		        LOG.info("Loading image with txid " + sig.mostRecentCheckpointTxId);
		        File file = bnStorage.findImageFile(NameNodeFile.IMAGE,
		            sig.mostRecentCheckpointTxId);
		        bnImage.reloadFromImageFile(file, checkerNode.getNamesystem());
		      }
		      //rollForwardByApplyingLogs(manifest, bnImage, checkerNode.getNamesystem());
		    }
		    
		    long txid = bnImage.getLastAppliedTxId();
		    
		    checkerNode.namesystem.writeLock();
		    try {
		    	checkerNode.namesystem.setImageLoaded();
		      if(checkerNode.namesystem.getBlocksTotal() > 0) {
		    	  checkerNode.namesystem.setBlockTotal();
		      }
		      bnImage.saveFSImageInAllDirs(checkerNode.getNamesystem(), txid);
		      bnStorage.writeAll();
		    } finally {
		    	checkerNode.namesystem.writeUnlock();
		    }

		    if(cpCmd.needToReturnImage()) {
		      TransferFsImage.uploadImageFromStorage(checkerNode.nnHttpAddress, conf,
		          bnStorage, NameNodeFile.IMAGE, txid);
		    }

		    getRemoteNamenodeProxy().endCheckpoint(checkerNode.getRegistration(), sig);

		    if (checkerNode.getRole() == NamenodeRole.BACKUP) {
		      bnImage.convergeJournalSpool();
		    }
		    checkerNode.setRegistration(); // keep registration up to date
		    
		    long imageSize = bnImage.getStorage().getFsImageName(txid).length();
		    LOG.info("Checkpoint completed in "
		        + (now() - startTime)/1000 + " seconds."
		        + " New Image Size: " + imageSize);*/
		  
	  }

	  
	  

	  
	  
}
