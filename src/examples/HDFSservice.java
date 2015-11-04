package examples;


/**
 * 
 */

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.typesafe.config.Config;
//import ConfigProvider;

/**
 * @author bikash
 *
 */
public class HdfsService {

	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsService.class);

	private static final String CORE_SITE = "/Users/bikash/BigData/hadoop/etc/hadoop/core-site.xml";
	private static final String HDFS_SITE = "/Users/bikash/BigData/hadoop/etc/hadoop/hdfs-site.xml";
	private static final String YARN_SITE = "/Users/bikash/BigData/hadoop/etc/hadoop/yarn-site.xml";
	private static final String MAPRED_SITE = "/Users/bikash/BigData/hadoop/etc/hadoop/mapred-site.xml";

	//private Config c;
	private FileSystem fs;

	public HdfsService() {
		//this.c = ConfigProvider.getConfig();
		String hadoopBase = "/Users/bikash/BigData/hadoop";
		Configuration conf = new Configuration();
		conf.addResource(new Path(hadoopBase.concat(CORE_SITE)));
		conf.addResource(new Path(hadoopBase.concat(HDFS_SITE)));
		conf.addResource(new Path(hadoopBase.concat(YARN_SITE)));
		conf.addResource(new Path(hadoopBase.concat(MAPRED_SITE)));
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String hdfsPath = "hdfs://localhost:9000"; 
		conf.set("fs.default.name", hdfsPath);
		try {
			this.fs = FileSystem.get(conf);
			LOGGER.info("Fs Created [{}]", fs.getUri());
		} catch (IOException e) {
			LOGGER.error("Error trying to init HDFS Service", e);
		}
	}

	public String createFile(InputStream content, String path) {
		String fullName = this.fs.getUri() + path + "/" + UUID.randomUUID();
		LOGGER.info("File To be written [{}]", fullName);
		Path p = new Path(fullName);
		try {
			FSDataOutputStream file = this.fs.create(p);
			file.write(IOUtils.toByteArray(content));
			file.close();
			return p.toString();
		} catch (IOException e) {
			LOGGER.error("Error trying to createFile", e);
			return "";
		}

	}

	public String getFile(String path) throws FileNotFoundException, IOException {
		String fullName = this.fs.getUri() + path;
		Path p = new Path(fullName);
		LOGGER.info("Get file with fullName [{}]", p);
		// FSDataInputStream data = this.fs.open(p);
		BufferedReader br = null;
		br = new BufferedReader(new InputStreamReader(fs.open(p)));

		String line;
		line = br.readLine();
		StringBuffer sb = new StringBuffer();
		while (line != null) {
			sb.append(line);
			sb.append("\n");
			LOGGER.info(line);
			line = br.readLine();
		}
		return sb.toString();
	}

	public boolean deleteFile(String path) {
		String fullName = this.fs.getUri() + path;
		LOGGER.info("Delete file with fullName [{}]", fullName);
		Path p = new Path(fullName);
		try {
			return this.fs.delete(p, true);
		} catch (IOException e) {
			LOGGER.error(String.format("Error trying to delete path: %s", p), e);
			return false;
		}
	}

	public FsStatus getStatus() {
		try {
			return this.fs.getStatus();
		} catch (IOException e) {
			LOGGER.error("Error trying to get FS Status", e);
			return null;
		}
	}

}
