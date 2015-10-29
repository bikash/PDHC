package org.apache.hadoop.fs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author bikash agrawal
 *
 */
public class ConfigProvider {

	private static Config c = ConfigFactory.load("conf/application.conf");

	public static Config getConfig() {
		return c;
	}

}