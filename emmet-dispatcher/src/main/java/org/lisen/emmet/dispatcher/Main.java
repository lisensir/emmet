package org.lisen.emmet.dispatcher;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.lisen.emmet.commons.util.SysProp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main implements Watcher {
	
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	
	enum DispatcherState {RUNNING, ELECTED, NO_ELECTED};
	
	@SuppressWarnings("unused")
	private volatile DispatcherState state = DispatcherState.RUNNING;
	
	private Random random = new Random(this.hashCode());
	
	private ZooKeeper zk;
	
	private String serverId = Integer.toHexString(random.nextInt());
	
	private volatile boolean connected = false;
	
	private volatile boolean expired = false;

	
	@Override
	public void process(WatchedEvent e) {
		
		logger.info("process event :" + e.toString());
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
				logger.info("session expired");
				break;
			default:
				break;
			
			}
		}
		
	}
	
	boolean isConnected() {
		return connected;
	}
	
	boolean isExpired() {
		return expired;
	}
	
	void startZk() throws IOException {
		zk = new ZooKeeper(SysProp.getZkServerAndPort(), 15000, this);
	}
	
	
	void stopZk() throws InterruptedException, IOException {
		zk.close();
	}
	
	
	//建立必要的节点
	public void bootstrap() {
		createParent("/workers", new byte[0]);
		createParent("/assign", new byte[0]);
		createParent("/tasks", new byte[0]);
		createParent("/status", new byte[0]);
	}
	
	void createParent(String path, byte[] data) {
		zk.create(
				path, 
				data, 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT,
				createParentCallback,
				data);
	}
	
	
	StringCallback createParentCallback = new StringCallback() {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				createParent(path, (byte[])ctx);
				break;
			case OK:
				logger.info(String.format("%s created ", path));
				break;
			case NODEEXISTS:
				logger.info(String.format("Parent already registered: %s", path));
				break;
			default:
				logger.info("somethings went wrong", KeeperException.create(Code.get(rc), path));
			}
		}
		
	};
	
	public void runForMaster() {
		logger.info("running for master...");
		zk.create(
				"/master", 
				serverId.getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.EPHEMERAL,
				masterCreateCallback,
				null);
	}
	
	
	StringCallback masterCreateCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				/*
				 * 如果发生连接丢失的情况则需要检查是否存在主节点，如果没有主节点则继续尝试成为主节点，
				 * 如果已存在了主节点则本节点成为备用节点
				 */
				checkMaster();
				break;
			case OK:
				state = DispatcherState.ELECTED;
				logger.info("我已经是主节点了....");
				break;
			case NODEEXISTS:
				/*
				 * 如果主节点已经存在，则本节点作为备用节点，同时需要在masterNode上设置监听，
				 * 以便于在主节点发生故障或网络丢失的情况时接替主节点
				 */
				state = DispatcherState.NO_ELECTED;
				logger.info("已经有主节点了，我将作为备用节点....");
				masterExists();
				break;
			default:
				state = DispatcherState.NO_ELECTED;
				logger.info("竞选主节点时发生了错误："+KeeperException.create(Code.get(rc),path));
			}
		}
	};
	
	
	void checkMaster() {
		zk.getData("/master", false, masterCheckback, null);
	}
	
	
	DataCallback masterCheckback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				checkMaster();
				break;
			case NONODE:
				runForMaster();
				break;
			case OK:
				if(serverId.equals(String.valueOf(data))) {
					state = DispatcherState.ELECTED;
					System.out.println("我是leader....");
					/*
					 * 执行领导者职能，从task节点获取任务列表，然后分配给可用的工作者
					 */
					
				} else {
					/*
					 * 如果该节点不是主节点，则需要在master节点上设置监听，以便于主节点发生故障或失去连接时，能
					 * 竞选主节点
					 */
					state = DispatcherState.NO_ELECTED;
					masterExists();
				}
			default:
				break;
			}
		}
	};
	
	
	/**
	 * 执行领导权
	 */
	void exceLeadership() {
		
	}
	
	/**
	 * 在本节点不能成为主节点的情况下，本节点需要监视master节点，以便于主节点在发生故障或失去连接时，能参与
	 * 主节点的竞选。
	 */
	void masterExists() {
		zk.exists("/master", masterExistsWatcher, masterExistsCallback,null);
	}
	
	/**
	 * 监视器，当master节点消失时（说明主节点发生故障），尝试成为主节点
	 */
	Watcher masterExistsWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent e) {
			if(e.getType() == Event.EventType.NodeDeleted) {
				assert "/master".equals(e.getPath());
				logger.info("master消失，立即尝试成为主节点...");
				runForMaster();
			}
		}
	};
	
	StatCallback masterExistsCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				masterExists();
				break;
			case OK:
				//设置监听成功
				break;
			case NONODE:
				//master节点消失了，立即尝试成为主节点
				state = DispatcherState.RUNNING;
				logger.info("master消失， 立即尝试成为主节点.. ..");
				runForMaster();
				break;
			default:
				checkMaster();
				break;
			}
		}
	};
	
	public static void main(String[] arg) throws Exception {
		
		Main main = new Main();
		main.startZk();
		
		while(!main.isConnected()) {
			Thread.sleep(1000);
		}
		
		main.bootstrap();
		main.runForMaster();
		
		while(!main.isExpired()) {
			Thread.sleep(1000);
		}
		
		main.stopZk();
	}

}
