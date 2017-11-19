package com.scistor.operator.utils;

import com.google.common.base.Objects;
import com.scistor.operator.pojo.SlavesLocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 
 * @description  zookeeper 操作类
 * @author zhujiulong
 * @date 2016年7月21日 上午11:42:47
 *
 */
public class ZKOperator implements RunningConfig {

	private static final Log LOG = LogFactory.getLog(ZKOperator.class);

	public static ZooKeeper getZookeeperInstance(String zookeeper_addr) throws IOException {
		final CountDownLatch cdl = new CountDownLatch(1);
		ZooKeeper zookeeper = new ZooKeeper(zookeeper_addr, RunningConfig.ZK_SESSION_TIMEOUT, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					cdl.countDown();
				}
			}
		});
		return zookeeper;
	}

	public static boolean checkPath(ZooKeeper zooKeeper, String path){
		try{
			if(zooKeeper.exists(path,null)==null) {
				return false;
			}else{
				return true;
			}
		}catch (Exception e ){
			LOG.error(e.toString());
		}
		return false;
	}

	public static List<SlavesLocation> getLivingSlaves(ZooKeeper zookeeper) throws KeeperException, InterruptedException{
		LOG.info("Getting living slaves in zookeeper...");
		if(Objects.equal(zookeeper, null)){
			throw new IllegalArgumentException("zookeeper instance is not available....,zookeeper=="+zookeeper);
		}
		List<SlavesLocation> list = new ArrayList<SlavesLocation>();
		List<String> children = zookeeper.getChildren(LIVING_SLAVES, false);
		for(String child : children){
			SlavesLocation loc = new SlavesLocation();
			loc.setIp(child.split(":")[0]);
			loc.setPort(Integer.parseInt(child.split(":")[1]));
			list.add(loc);
		}
		return list;
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		final CountDownLatch cdl = new CountDownLatch(1);
		ZooKeeper zooKeeper = new ZooKeeper("172.16.18.18:2181",10000,new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				cdl.countDown();
			}
		});
	}

}
