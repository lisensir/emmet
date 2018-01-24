package org.lisen.emmet.worker;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.lisen.emmet.commons.util.SysProp;

public class Worker implements Watcher {
	
	private Logger logger = Logger.getLogger(this.getClass());
	
	private ZooKeeper zk;
	
	private String serverId = Integer.toHexString((new Random()).nextInt());
	
	private final String ZK_SERV_PORT = SysProp.getZkServerAndPort();
	
	private final int ZK_SESSION_TIME_OUT = SysProp.getSessionTimeout();
	
	private volatile boolean connected = false;
	
	private volatile boolean expired = false;
	
	private ThreadPoolExecutor executor;
	
	public Worker() {
		this.executor = new ThreadPoolExecutor(1,
				1,1000L, 
				TimeUnit.MILLISECONDS, 
				new ArrayBlockingQueue<Runnable>(200));
	}
	
	public void startZk() throws IOException {
		zk = new ZooKeeper(ZK_SERV_PORT, ZK_SESSION_TIME_OUT,  this);
	}
	
	public boolean isConnected() {
        return connected;
    }
    
    public boolean isExpired() {
        return expired;
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
	
	
	private String name = "worker-" + serverId;
	
	/**
	 * 工作者启动后将自身注册到注册中心，以便于接收任务
	 */
	public void register() {
		zk.create("/workers/"+name, 
				"idle".getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL,
				createWorkerCallback,
				null);
	}
	
	StringCallback createWorkerCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				register();
				break;
			case OK:
				logger.info(String.format("worker: %s 注册成功", name));
				break;
			case NODEEXISTS:
				logger.info(String.format("work: %s  已经注册", name));
				break;
			default:
				logger.info("somethis is wrong");
				break;
			}
			
		}
	};
	
	
	/**
	 * 为工作者建立任务分配节点，用于存放分配的任务
	 */
	void createAssignNode() {
		zk.create("/assign/"+name, 
				new byte[0], 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT,
				createAssignCallback,
				null);
	}
	
	
	StringCallback createAssignCallback = new StringCallback() {
		public void processResult(int rc, String path,Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				createAssignNode();
				break;
			case OK:
				logger.info(String.format("worker: %s, 任务分配节点建立成功", name));
				break;
			case NODEEXISTS:
				logger.info(String.format("worker: %s, 任务分配节点已经创建", name));
				break;
			default:
				logger.info("something is wrong");
				break;
			}
		}
	};
	
	
	public static void main(String[] arg) throws IOException, InterruptedException {
		Worker worker = new Worker();
		worker.startZk();
		
		while(!worker.isConnected()) {
			Thread.sleep(1000);
		}
		
		worker.createAssignNode();
		
		worker.register();
		
		while(!worker.isExpired()) {
			Thread.sleep(1000);
		}
		
	}
	
	

}
