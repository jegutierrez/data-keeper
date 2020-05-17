package io.jegutierrez.client;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class ZooKeeperConnection {
    
    private ZooKeeper zk;
    final CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZooKeeper connect(String host, int port, int timeout) throws IOException, InterruptedException {

        zk = new ZooKeeper(String.format("%s:%d", host, port), timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        
        connectedSignal.await();
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}