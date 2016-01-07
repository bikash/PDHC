package examples;

//Author: Bikash
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LOGGER.info("Starting application");

		//Config c = ConfigProvider.getConfig();
		//port("9000");
		//String base = "/Users/bikash/repos/bigdata/hadoop-common/hadoop-hdfs-project";
		HdfsService hdfsService = new HdfsService();
		//LOGGER.info("Application Started!");
		
	}

}
