package io.jegutierrez;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.NotEmpty;

public class DataKeeperConfiguration extends Configuration {
    @NotEmpty
    private String nodeName;

    @NotEmpty
    private String zooKeeperAddress;

    @NotNull
    private Integer zooKeeperPort;

    @NotNull
    private Integer zooKeeperTimeout;

    @JsonProperty
    public String getNodeName() {
        return nodeName;
    }

    @JsonProperty
    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    @JsonProperty
    public String getZooKeeperAddress() {
        return zooKeeperAddress;
    }

    @JsonProperty
    public void setZooKeeperAddress(String zooKeeperAddress) {
        this.zooKeeperAddress = zooKeeperAddress;
    }

    @JsonProperty
    public Integer getZooKeeperPort() {
        return zooKeeperPort;
    }

    @JsonProperty
    public void setZooKeeperPort(Integer zooKeeperPort) {
        this.zooKeeperPort = zooKeeperPort;
    }

    @JsonProperty
    public Integer getZooKeeperTimeout() {
        return zooKeeperTimeout;
    }

    @JsonProperty
    public void setZooKeeperTimeout(Integer zooKeeperTimeout) {
        this.zooKeeperTimeout = zooKeeperTimeout;
    }
}
