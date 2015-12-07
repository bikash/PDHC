package examples;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyDefault;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.http.HttpConfig;

/** 
 * This class contains constants for configuration keys used
 * in hdfs.
 *
 */

@InterfaceAudience.Private
public class ConfigKey extends CommonConfigurationKeys {
	  public static final String  DFS_NAMENODE_CHECKER_HTTP_ADDRESS_KEY = "dfs.namenode.secondary.http-address";
	  public static final String  DFS_NAMENODE_CHECKER_HTTP_ADDRESS_DEFAULT = "0.0.0.0:60090";
	  public static final String  DFS_NAMENODE_CHECKER_HTTPS_ADDRESS_KEY = "dfs.namenode.secondary.https-address";
	  public static final String  DFS_NAMENODE_CHECKER_HTTPS_ADDRESS_DEFAULT = "0.0.0.0:60091";
	
}
