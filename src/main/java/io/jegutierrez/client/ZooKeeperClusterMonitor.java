package io.jegutierrez.client;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.DataKeeperClusterInfo;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ZooKeeperClusterMonitor {
    public final String CLUSTER = "/cluster";
    public final String NODES = "/nodes";
    public final String LIVE_NODES = "/live-nodes";
    public final String LEADER = "/leader";

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterMonitor.class);
    boolean connected;
    ZooKeeper zk;
    DataKeeperClusterInfo clusterInfo;

    ZooKeeperClusterMonitor(ZooKeeper zk, DataKeeperClusterInfo clusterInfo) {
        this.zk = zk;
        this.connected = true;
        this.clusterInfo = clusterInfo;
        createClusterNodes();
        zk.exists(String.format("%s%s%s", CLUSTER, NODES, clusterInfo.getNodeName()), getNodeRegistrationWatcher(),
                getNodeRegistrationCallback(zk), null);
        zk.exists(String.format("%s%s%s", CLUSTER, LIVE_NODES, clusterInfo.getNodeName()), getNodeRegistrationWatcher(),
                getNodeRegistrationCallback(zk), null);
        zk.exists(String.format("%s%s", CLUSTER, LEADER), getLeaderElectionWatcher(), getLeaderRegisterCallback(zk), null);
    }

    private void createClusterNodes() {
        try {
            if (zk.exists(CLUSTER, true) == null) {
                zk.create(CLUSTER, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(NODES, true) == null) {
                zk.create(NODES, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists(LIVE_NODES, true) == null) {
                zk.create(LIVE_NODES, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            log.error("could not create cluster nodes, error: %s", e.getMessage());
            e.printStackTrace();
        }
    }

    private Watcher getNodeRegistrationWatcher() {
        return new Watcher() {
			@Override
			public void process(WatchedEvent event) {
                log.debug("checking if node exists");
                log.debug(event.toString());
                if(event.getType() == Event.EventType.NodeCreated) {
                    log.debug("node %s created", event.getPath());
                } else if(event.getType() == Event.EventType.None) {
                    log.debug("another event on node %s", event.getPath());
                }
            }  
        };
    }

    private Watcher getLeaderElectionWatcher() {
        return new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				System.out.println(event);
                if (event.getType() == Event.EventType.None) {
                    switch (event.getState()) {
                        case SyncConnected:
                            connected = true;
                            break;
                        case Disconnected:
                            connected = false;
                            break;
                        case Expired:
                            connected = false;
                            System.out.println("Exiting due to session expiration");
                        default:
                            break;
                    }
                }
            }  
        };
    }

    private StatCallback getLeaderRegisterCallback(ZooKeeper zk) {
        return new StatCallback(){
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                boolean exists;
                switch (Code.get(rc)) {
                    case OK:
                        exists = true;
                        break;
                    case NONODE:
                        exists = false;
                        break;
                    case SESSIONEXPIRED:
                    case NOAUTH:
                        connected = false;
                    return;
                default:
                    // Retry errors
                    zk.exists(path, true, this, null);
                    return;
                }

                byte b[] = null;
                if (exists) {
                    try {
                        b = zk.getData(path, false, null);
                        clusterInfo.setMasterHostName(new String(b));
                    } catch (KeeperException e) {
                        // We don't need to worry about recovering now. The watch
                        // callbacks will kick off any exception handling
                        log.error("error getting cluster leader", e);
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        return;
                    }
                } else {
                    zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, getGenericCreationCallback(zk), 0);
                }
                if(b != null) {
                    log.info(new String(b));
                }
            }
        };
    }

    private StringCallback getGenericCreationCallback(ZooKeeper zk) {
        return new StringCallback(){
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                int count = ctx == null ? 0 : (int)ctx;
                switch (Code.get(rc)) { 
                    case CONNECTIONLOSS:
                        if(count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, getGenericCreationCallback(zk), count + 1);
                        } else {
                            log.info("Max creation retries to create node " + path);
                        }
                        break;
                    case OK:
                        log.info(path + " created");
                        break;
                    case NONODE:
                        log.info("NONODE, trying to create node " + path);
                        if(count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, getGenericCreationCallback(zk), count + 1);
                        } else {
                            log.info("Max creation retries to create node " + path);
                        }
                        break;
                    default:
                        log.error("Something went wrong" + KeeperException.create(Code.get(rc), path));
                    }
            }
        };
    }

    private StatCallback getNodeRegistrationCallback(ZooKeeper zk) {
        return new StatCallback(){
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                int count = ctx == null ? 0 : (int)ctx;
                switch (Code.get(rc)) { 
                case CONNECTIONLOSS:
                    /*
                     * Handling connection loss for a sequential node is a bit
                     * delicate. Executing the ZooKeeper create command again
                     * might lead to duplicate Masters. For now, let's assume
                     * that it is ok to create a duplicate Master.
                     */
                    if(count <= 5) {
                        zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, getGenericCreationCallback(zk), count + 1);
                    } else {
                        log.info("Max creation retries to create node " + path);
                    }
                    break;
                case OK:
                    log.info("Master created " + path);
                    break;
                case NONODE:
                    log.info("NONODE, trying to create node " + path);
                    if(count <= 5) {
                        zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, getGenericCreationCallback(zk), count + 1);
                    } else {
                        log.info("Max creation retries to create node " + path);
                    }
                    break;
                default:
                    log.error("Something went wrong" + KeeperException.create(Code.get(rc), path));
                }
            }
        };
    }
}