package io.jegutierrez;

import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import javax.validation.Valid;
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

    @Valid
    @NotNull
    private HttpClientConfiguration httpClient = new HttpClientConfiguration();

    @JsonProperty("httpClient")
    public HttpClientConfiguration getHttpClientConfiguration() {
        return httpClient;
    }

    @JsonProperty("httpClient")
    public void setHttpClientConfiguration(HttpClientConfiguration httpClient) {
        this.httpClient = httpClient;
    }
}
