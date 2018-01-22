package org.lisen.emmet.commons.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

public class SysProp {

	private static Properties prop = null;

	private static String SERVER_PORT = "zookeeper.serverports";
	
	private static String SESSION_TIME_OUT = "zookeeper.sessionTimeout";

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
	 * @return
	 */
	public static String getZkServerAndPort() {

		return prop.getProperty(SERVER_PORT);
	}
	
	/**
	 * 获取ZK服务器会话超时时间
	 * @return
	 */
	public static int getSessionTimeout() {
		return StringUtils.isBlank(prop.getProperty(SESSION_TIME_OUT)) ? 15000
				: Integer.parseInt(prop.getProperty(SESSION_TIME_OUT));
	}

}
