package org.lisen.emmet.dispatcher;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ClearNodes {
	
	public static void main(String[] arg) throws IOException, KeeperException, InterruptedException {
		
		ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 1500, new Watcher() {
			public void process(WatchedEvent e) {
				if(e.getType() ==  null) {
					return;
				}
				System.out.println("已经触发了" + e.getType() + "事件！");
			}
		});
		
		List<String> lst = zk.getChildren("/", true);
		
		if(lst == null || lst.size() < 0) return;
		
		System.out.println("==========================================");
		System.out.println("ZooKeeper中node节点: ");
		for(String node: lst) {
			System.out.println("node name : " + node);
		}
		
		//节点清理
		for(String node: lst) {
			if("zookeeper".equals(node)) continue;
			zk.delete("/"+node,-1);
		}
		
		System.out.println("==========================================");
		System.out.println("清理之后的node节点：");
		List<String> l = zk.getChildren("/", true);
		for(String node: l) {
			System.out.println("node name : " + node);
		}
		
	}

}
