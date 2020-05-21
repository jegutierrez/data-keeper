package io.jegutierrez;

import io.jegutierrez.resources.ClusterResource;
import io.jegutierrez.resources.DatabaseResource;
import io.jegutierrez.resources.NodeResource;
import io.jegutierrez.client.ZooKeeperClusterExecutor;
import io.jegutierrez.client.ZooKeeperConnection;
import io.jegutierrez.core.DataKeeperClusterInfo;
import io.jegutierrez.db.DatabaseRepository;

import io.dropwizard.Application;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.IOException;
import java.security.Security;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.MoreObjects;

import org.apache.http.client.HttpClient;
import org.apache.zookeeper.ZooKeeper;
import org.conscrypt.OpenSSLProvider;

public class DataKeeperApplication extends Application<DataKeeperConfiguration> {
    static {
        Security.insertProviderAt(new OpenSSLProvider(), 1);
    }
    private static DataKeeperClusterInfo clusterInfo;
    private static ZooKeeper zk;
    private static DatabaseRepository kvs;
    private static HttpClient httpClient;
    final static CountDownLatch appStarted = new CountDownLatch(1);

    public static void main(final String[] args) throws Exception {
        new DataKeeperApplication().run(args);

        appStarted.await();

        new ZooKeeperClusterExecutor(zk, clusterInfo, kvs, httpClient).run();
    }

    @Override
    public String getName() {
        return "DataKeeper";
    }

    @Override
    public void initialize(final Bootstrap<DataKeeperConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider((ConfigurationSourceProvider) new SubstitutingSourceProvider(
                bootstrap.getConfigurationSourceProvider(), new EnvironmentVariableSubstitutor(false))
        );
    }

    @Override
    public void run(final DataKeeperConfiguration appConfig, final Environment environment) throws IOException,
            InterruptedException {

        
        final HttpConnectorFactory applicationConnector = ((HttpConnectorFactory)
                ((DefaultServerFactory) appConfig.getServerFactory()).getApplicationConnectors().get(0));

        String host = MoreObjects.firstNonNull(applicationConnector.getBindHost(), "127.0.0.1");
        int port = applicationConnector.getPort();

        clusterInfo = new DataKeeperClusterInfo(
            appConfig.getNodeName(),
            host,
            port,
            appConfig.getZooKeeperAddress(), 
            appConfig.getZooKeeperPort()
        );
        zk = new ZooKeeperConnection().connect(
            appConfig.getZooKeeperAddress(), 
            appConfig.getZooKeeperPort(), 
            appConfig.getZooKeeperTimeout()
        );
        kvs = new DatabaseRepository();
        httpClient = new HttpClientBuilder(environment).using(appConfig.getHttpClientConfiguration())
                                                                    .build(getName());
        
        final DatabaseResource dbResource = new DatabaseResource(kvs, clusterInfo, httpClient);
        final ClusterResource clusterResource = new ClusterResource(zk, clusterInfo);
        final NodeResource nodeResource = new NodeResource();

        environment.jersey().register(dbResource);
        environment.jersey().register(clusterResource);
        environment.jersey().register(nodeResource);
        appStarted.countDown();
    }

}
