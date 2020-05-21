package io.jegutierrez.core;

public class ClusterNode {
    private String hostName;
    private String address;
    private int port;

    public ClusterNode(String hostName, String address, int port) {
        this.hostName = hostName;
        this.address = address;
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}