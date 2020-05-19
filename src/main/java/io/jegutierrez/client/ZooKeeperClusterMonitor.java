package io.jegutierrez.client;

import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.DataKeeperClusterInfo;

public class ZooKeeperClusterMonitor {
    public final String CLUSTER = "cluster";
    public final String NODES = "nodes";
    public final String LIVE_NODES = "live-nodes";
    public final String LEADER = "leader";
    public final String ELECTED = "elected";

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterMonitor.class);
    boolean connected;
    ZooKeeper zk;
    DataKeeperClusterInfo clusterInfo;

    ZooKeeperClusterMonitor(ZooKeeper zk, DataKeeperClusterInfo clusterInfo) {
        this.zk = zk;
        this.connected = true;
        this.clusterInfo = clusterInfo;
        createClusterNodes();
        getClusterNodes(0);
        zk.exists(String.format("/%s/%s/%s", CLUSTER, LIVE_NODES, clusterInfo.getNodeName()),
                getNodeRegistrationWatcher(), getNodeRegistrationCallback(zk), null);
        getLeader(0);
    }

    private void createClusterNodes() {
        try {
            if (zk.exists("/" + CLUSTER, true) == null) {
                zk.create("/" + CLUSTER, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists("/" + CLUSTER + "/" + LEADER, true) == null) {
                zk.create("/" + CLUSTER + "/" + LEADER, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists("/" + CLUSTER + "/" + NODES, true) == null) {
                zk.create("/" + CLUSTER + "/" + NODES, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (zk.exists("/" + CLUSTER + "/" + LIVE_NODES, true) == null) {
                zk.create("/" + CLUSTER + "/" + LIVE_NODES, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            log.error("could not create cluster nodes, error: %s", e.getMessage());
            e.printStackTrace();
        }
    }

    private Watcher getLeaderWatcher() {
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    getLeader(0);
                }
            }
        };
    }

    private void getLeader(int retryCount) {
        zk.getChildren(String.format("/%s/%s", CLUSTER, LEADER), getLeaderWatcher(), getLeaderCallback(zk), retryCount);
    }

    private void takeLeadership(int retryCount) {
        zk.create(String.format("/%s/%s/%s", CLUSTER, LEADER, ELECTED), clusterInfo.getNodeData().getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, takeLeadershipCallback, retryCount);
    }

    private ChildrenCallback getLeaderCallback(ZooKeeper zk) {
        return new ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                int retryCount = ctx == null ? 0 : (int) ctx;
                if (retryCount > 5) {
                    log.error("Max retries reached" + path);
                }
                switch (Code.get(rc)) {
                    case OK:
                        if (children.size() > 0) {
                            try {
                                byte[] leaderData = zk.getData(path + "/" + children.get(0), false, null);
                                clusterInfo.setLeader(new String(leaderData));
                                log.info("Leader elected, node: " + new String(leaderData));
                            } catch (KeeperException | InterruptedException e) {
                                log.error("error getting cluster leader stats", KeeperException.create(Code.get(rc), path));
                            }
                        } else {
                            log.info("No leader elected, taking leadership");
                            takeLeadership(0);
                        }
                        break;
                    case CONNECTIONLOSS:
                        log.error("error getting cluster leader stats");
                        log.info("retrying to get cluster leader stats...");
                        getLeader(retryCount + 1);
            
                        break;
                    default:
                        log.error("getChildren failed",KeeperException.create(Code.get(rc), path));
                }
            }
        };
    }

    StringCallback takeLeadershipCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            int retryCount = ctx == null ? 0 : (int) ctx;
            if (retryCount > 5) { 
                log.error("Max retries reached" + path);
            }
            switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                takeLeadership(retryCount + 1);
                break;
            case OK:
                log.info("leader elected");
                getLeader(0);
                break;
            case NODEEXISTS:
                log.warn("leader exists");
                getLeader(0);
                break;
            default:
                log.error("Something went wrong: " + KeeperException.create(Code.get(rc), path));
            }
        }
    };

    private void getClusterNodes(int retryCount) {
        zk.exists(
            String.format("/%s/%s/%s", CLUSTER, NODES, clusterInfo.getNodeName()), 
            getNodeRegistrationWatcher(),
            getNodeRegistrationCallback(zk), 
            retryCount
        );
    }

    private void joinCluster(int retryCount){
        zk.create(
            String.format("/%s/%s/%S", CLUSTER, NODES, clusterInfo.getNodeName()), 
            clusterInfo.getNodeName().getBytes(), 
            Ids.OPEN_ACL_UNSAFE, 
            CreateMode.EPHEMERAL,
            takeLeadershipCallback,
            retryCount
        );
    }

    private Watcher getNodeRegistrationWatcher() {
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                log.debug("checking if node exists");
                System.out.println("------------------------------");
                System.out.println("-------- NODE UPDATE ---------");
                System.out.println(event);
                System.out.println("------------------------------");
                log.debug(event.toString());
                if (event.getType() == Event.EventType.NodeCreated) {
                    log.debug("node %s created", event.getPath());
                } else if (event.getType() == Event.EventType.None) {
                    log.debug("another event on node %s", event.getPath());
                }
            }
        };
    }

    // private StringCallback getLeaderCreationCallback(ZooKeeper zk) {
    //     return new StringCallback() {
    //         @Override
    //         public void processResult(int rc, String path, Object ctx, String name) {
    //             int count = ctx == null ? 0 : (int) ctx;
    //             switch (Code.get(rc)) {
    //                 case CONNECTIONLOSS:
    //                     if (count <= 5) {
    //                         zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
    //                                 CreateMode.EPHEMERAL, getLeaderCreationCallback(zk), count + 1);
    //                     } else {
    //                         log.info("Max creation retries to create node " + path);
    //                     }
    //                     break;
    //                 case OK:
    //                     log.info(path + " created");
    //                     zk.exists(path, getLeaderWatcher(), getLeaderRegisterCallback(zk), null);
    //                     break;
    //                 case NONODE:
    //                     log.info("NONODE, trying to create node " + path);
    //                     if (count <= 5) {
    //                         zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
    //                                 CreateMode.EPHEMERAL, getLeaderCreationCallback(zk), count + 1);
    //                     } else {
    //                         log.info("Max creation retries to create node " + path);
    //                     }
    //                     break;
    //                 default:
    //                     log.error("Something went wrong" + KeeperException.create(Code.get(rc), path));
    //             }
    //         }
    //     };
    // }

    private StringCallback getGenericCreationCallback(ZooKeeper zk) {
        return new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                int count = ctx == null ? 0 : (int) ctx;
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        if (count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL, getGenericCreationCallback(zk), count + 1);
                        } else {
                            log.info("Max creation retries to create node " + path);
                        }
                        break;
                    case OK:
                        log.info(path + " created");
                        break;
                    case NONODE:
                        log.info("NONODE, trying to create node " + path);
                        if (count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL, getGenericCreationCallback(zk), count + 1);
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
        return new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                int count = ctx == null ? 0 : (int) ctx;
                switch (Code.get(rc)) {
                    case CONNECTIONLOSS:
                        if (count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL, getGenericCreationCallback(zk), count + 1);
                        } else {
                            log.info("Max creation retries to create node " + path);
                        }
                        break;
                    case OK:
                        log.info("Node created " + path);
                        break;
                    case NONODE:
                        log.info("NONODE, trying to create node " + path);
                        if (count <= 5) {
                            zk.create(path, clusterInfo.getNodeName().getBytes(), Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.EPHEMERAL, getGenericCreationCallback(zk), count + 1);
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