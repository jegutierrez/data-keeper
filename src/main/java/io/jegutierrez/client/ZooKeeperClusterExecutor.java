package io.jegutierrez.client;

import java.io.IOException;

import org.apache.http.client.HttpClient;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.DataKeeperClusterInfo;
import io.jegutierrez.db.DatabaseRepository;

public class ZooKeeperClusterExecutor implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterExecutor.class);

    ZooKeeperClusterManager clusterMonitor;
    ZooKeeper zk;
    DataKeeperClusterInfo clusterInfo;
    DatabaseRepository kvs;
    HttpClient httpClient;

    public ZooKeeperClusterExecutor(ZooKeeper zk, DataKeeperClusterInfo clusterInfo, DatabaseRepository kvs, HttpClient httpClient) throws IOException {
        this.zk = zk;
        this.clusterInfo = clusterInfo;
        this.kvs = kvs;
        this.httpClient = httpClient;
        clusterMonitor = new ZooKeeperClusterManager(zk, clusterInfo, kvs, httpClient);
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