package org.lisen.emmet.commons.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SysProp {

	private static Properties prop = null;

	private static String SERVER_PORT = "zookeeper.serverports";

	static {
		InputStream in = Object.class.getResourceAsStream("/sys.properties");
		prop = new Properties();
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取ZK地址及端口号
	 * 
	 * @return
	 */
	public static String getZkServerAndPort() {

		return prop.getProperty(SERVER_PORT);
	}

}
