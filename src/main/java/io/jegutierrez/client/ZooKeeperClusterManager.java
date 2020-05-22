package io.jegutierrez.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.jegutierrez.core.ClusterNode;
import io.jegutierrez.core.DataKeeperClusterInfo;
import io.jegutierrez.db.DatabaseRepository;

public class ZooKeeperClusterManager {
    public final String CLUSTER = "cluster";
    public final String NODES = "nodes";
    public final String LEADER = "leader";
    public final String ELECTED = "elected";

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperClusterManager.class);
    boolean connected;
    ZooKeeper zk;
    DataKeeperClusterInfo clusterInfo;
    DatabaseRepository kvs;
    HttpClient httpClient;
    ObjectMapper jsonMapper;

    ZooKeeperClusterManager(ZooKeeper zk, DataKeeperClusterInfo clusterInfo, DatabaseRepository kvs,
            HttpClient httpClient) {
        this.zk = zk;
        this.connected = true;
        this.clusterInfo = clusterInfo;
        this.kvs = kvs;
        this.httpClient = httpClient;
        this.jsonMapper = new ObjectMapper();
        createClusterNodesIfNodeExist();
        joinCluster(0);
        getClusterNodes(0);
        getLeader(0);
    }

    private void createClusterNodesIfNodeExist() {
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
                                syncWithLeader(retryCount);

                            } catch (KeeperException | InterruptedException | IOException e) {
                                log.error("error getting cluster leader stats",
                                        KeeperException.create(Code.get(rc), path));
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
                        log.error("getChildren failed", KeeperException.create(Code.get(rc), path));
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

    private void syncWithLeader(int retryCount) throws ClientProtocolException, IOException {    
        if(clusterInfo.imILeader()) {
            return;
        }
        String url = String.format("http://%s:%d/data/sync", clusterInfo.getLeaderAddress(), clusterInfo.getLeaderPort());
        log.info("sync data request started " +url);
        HttpGet request = new HttpGet(url);
        HttpResponse response;
        response = httpClient.execute(request);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
            // retry
            getLeader(retryCount + 1);
            log.error("error getting data from leader");
        }
        String data = EntityUtils.toString(response.getEntity());
        HashMap<String, String> result = jsonMapper.readValue(data, HashMap.class);
        log.info("sync data request completed " +data);
        kvs.syncData(result);
    }

    private Watcher liveNodesWatcher() {
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    getClusterNodes(0);
                }
            }
        };
    }

    private void getClusterNodes(int retryCount) {
        zk.getChildren(String.format("/%s/%s", CLUSTER, NODES), liveNodesWatcher(), getNodesCallback(zk), retryCount);
    }

    private void joinCluster(int retryCount) {
        zk.create(String.format("/%s/%s/%S", CLUSTER, NODES, clusterInfo.getNodeName()),
                clusterInfo.getNodeData().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, clusterJoinCallback,
                retryCount);
    }

    private ChildrenCallback getNodesCallback(ZooKeeper zk) {
        return new ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                int retryCount = ctx == null ? 0 : (int) ctx;
                if (retryCount > 5) {
                    log.error("Max retries reached" + path);
                }
                switch (Code.get(rc)) {
                    case OK:
                        try {
                            List<ClusterNode> liveNodes = new ArrayList<>();
                            for (String node : children) {
                                log.info(node);
                                byte[] nodeData = zk.getData(path + "/" + node, false, null);
                                log.info(new String(nodeData));
                                String[] data = new String(nodeData).split(";");
                                ClusterNode clusterNode = new ClusterNode(data[0], data[1], Integer.parseInt(data[2]));
                                liveNodes.add(clusterNode);
                            }
                            clusterInfo.setLiveNodes(liveNodes);
                        } catch (KeeperException | InterruptedException e) {
                            log.error("error getting cluster nodes", KeeperException.create(Code.get(rc), path));
                        }
                        break;
                    case CONNECTIONLOSS:
                        log.error("error getting cluster nodes");
                        log.info("retrying request...");
                        getClusterNodes(retryCount + 1);
                        break;
                    default:
                        log.error("could not get cluster nodes", KeeperException.create(Code.get(rc), path));
                }
            }
        };
    }

    StringCallback clusterJoinCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            int retryCount = ctx == null ? 0 : (int) ctx;
            if (retryCount > 5) {
                log.error("Max retries reached" + path);
            }
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    joinCluster(retryCount + 1);
                    break;
                case OK:
                    log.info("successfully joined");
                    getClusterNodes(0);
                    break;
                case NODEEXISTS:
                    log.warn("already joined");
                    getClusterNodes(0);
                    break;
                default:
                    log.error("could not join to the cluster " + KeeperException.create(Code.get(rc), path));
            }
        }
    };
}