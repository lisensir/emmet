package org.lisen.emmet.worker;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.lisen.emmet.commons.util.SysProp;

public class Worker implements Watcher {
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private ZooKeeper zk;
	
	private final String ZK_SERV_PORT = SysProp.getZkServerAndPort();
	
	private final int ZK_SESSION_TIME_OUT = SysProp.getSessionTimeout();
	
	private volatile boolean connected = false;
	
	private volatile boolean expired = false;
	
	public void startZk() throws IOException {
		zk = new ZooKeeper(ZK_SERV_PORT, ZK_SESSION_TIME_OUT,  this);
	}
	
	public void process(WatchedEvent e) {
		if(e.getType() == Event.EventType.None) {
			switch(e.getState()) {
			case SyncConnected:
				connected = true;
				break;
			case Disconnected:
				connected = false;
				break;
			case Expired:
				expired = true;
				connected = false;
				logger.info("会话超时");
				break;
			default:
				break;
			}
		}
	}
	
	
	/**
	 * 工作者启动后将自身注册到注册中心，以便于接收任务
	 */
	public void register() {
		
	}

}
