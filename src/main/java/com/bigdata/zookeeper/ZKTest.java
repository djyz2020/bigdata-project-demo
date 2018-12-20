package com.bigdata.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKTest {
	
	private static String connectString = "192.168.106.169:2181";
	
	public static void main(String[] args) throws IOException {
		
		ZooKeeper zk = new ZooKeeper(connectString, 3000, new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.out.println("事件类型：" + event.getType());
				System.out.println("事件状态： " + event.getState());
				
			}
		});
		
		while(!"CONNECTED".equals(zk.getState().toString())){
			System.out.println(zk.getState());
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("zookeeper已成功连接上！");
		try {
			List<String> children = zk.getChildren("/", true);
			if(children.size() > 0){
				for(String child : children){
					System.out.println("child: " + child);
					System.out.println(zk.getChildren("/" + child, true));
				}
			}
			
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
}



