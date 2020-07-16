package io.jegutierrez.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataKeeperClusterInfo {
    private final String nodeName;
    private final String nodeAddress;
    private final int nodePort;
    private final String zooKeeperAddress;
    private final int zooKeeperPort;

    private List<ClusterNode> liveNodes;
    private String leaderHostName;
    private String leaderAddress;
    private int leaderPort;

    public DataKeeperClusterInfo(String nodeName, String nodeAddress, int nodePort, String zooKeeperAddress, int zooKeeperPort) {
        this.nodeName = nodeName;
        this.nodeAddress = nodeAddress;
        this.nodePort = nodePort;
        this.zooKeeperAddress = zooKeeperAddress;
        this.zooKeeperPort = zooKeeperPort;
        this.liveNodes = new ArrayList<>();
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public int getNodePort() {
        return nodePort;
    }

    public String getNodeData() {
        return String.format("%s;%s;%d", nodeName, nodeAddress, nodePort);
    }

    public String getLeaderHostName() {
        return leaderHostName;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public int getLeaderPort() {
        return leaderPort;
    }

    public void setLeader(String leaderData) {
        System.out.println(leaderData);
        String[] data = leaderData.split(";");
        this.leaderHostName = data[0];
        this.leaderAddress = data[1];
        this.leaderPort = Integer.parseInt(data[2]);
    }

    public Map<String, String> getLeaderMapData() {
        return Map.of(
            "host-name", leaderHostName,
            "address", leaderAddress,
            "port", ""+leaderPort
        );
    }

    public boolean imILeader() {
        return this.nodeName.equals(this.leaderHostName);
    }

    public String getZooKeeperAddress() {
        return zooKeeperAddress;
    }

    public int getZooKeeperPort() {
        return zooKeeperPort;
    }

    public List<ClusterNode> getLiveNodes() {
        return liveNodes;
    }

    public void setLiveNodes(List<ClusterNode> liveNodes) {
        this.liveNodes = liveNodes;
    }

    public List<Map<String, String>> getLiveNodesMapData() {
        return this.liveNodes.stream().map((ClusterNode node) -> Map.of(
            "host-name", node.getHostName(),
            "address", node.getAddress(),
            "port", ""+node.getPort(),
            "leader", ""+node.getHostName().equals(leaderHostName)
        )).collect(Collectors.toList());
    }
}