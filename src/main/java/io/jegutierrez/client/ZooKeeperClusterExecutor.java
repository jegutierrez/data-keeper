package io.jegutierrez.client;

import java.io.IOException;

import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.DataKeeperClusterInfo;

public class ZooKeeperClusterExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterExecutor.class);

    ZooKeeperClusterMonitor clusterMonitor;
    ZooKeeper zk;
    DataKeeperClusterInfo clusterInfo;

    public ZooKeeperClusterExecutor(ZooKeeper zk, DataKeeperClusterInfo clusterInfo) throws IOException {
        this.zk = zk;
        this.clusterInfo = clusterInfo;
        clusterMonitor = new ZooKeeperClusterMonitor(zk, clusterInfo);
        log.info("zookeeper cluster executor started");
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                while (clusterMonitor.connected) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            log.error("cluster monitor interrupted");
        }
    }
    
}