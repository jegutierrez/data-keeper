package io.jegutierrez.core;

import java.util.ArrayList;
import java.util.List;

public class DataKeeperClusterInfo {
    private final String nodeName;
    private final String zooKeeperHostName;
    private final int zooKeeperPort;
    
    private List<String> liveNodes;
    private String masterHostName;

    public DataKeeperClusterInfo(String nodeName, String zookeeperHostName, int zooKeeperPort) {
        this.nodeName = nodeName;
        this.zooKeeperHostName = zookeeperHostName;
        this.zooKeeperPort = zooKeeperPort;
        this.liveNodes = new ArrayList<>();
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getMasterHostName() {
        return masterHostName;
    }

    public void setMasterHostName(String masterHostName) {
        this.masterHostName = masterHostName;
    }

    public boolean imIMaster() {
        return this.nodeName.equals(this.masterHostName);
    }

    public String getZooKeeperHostName() {
        return zooKeeperHostName;
    }

    public int getZooKeeperPort() {
        return zooKeeperPort;
    }

    public List<String> getLiveNodes() {
        return liveNodes;
    }

    public void setLiveNodes(List<String> liveNodes) {
        this.liveNodes = liveNodes;
    }

}