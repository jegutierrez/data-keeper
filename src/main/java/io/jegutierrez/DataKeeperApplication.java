package io.jegutierrez;

import io.jegutierrez.resources.ClusterResource;
import io.jegutierrez.resources.DatabaseResource;
import io.jegutierrez.resources.NodeResource;
import io.jegutierrez.client.ZooKeeperClusterExecutor;
import io.jegutierrez.client.ZooKeeperConnection;
import io.jegutierrez.core.DataKeeperClusterInfo;
import io.jegutierrez.db.DatabaseRepository;

import io.dropwizard.Application;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.IOException;
import java.security.Security;

import org.apache.zookeeper.ZooKeeper;
import org.conscrypt.OpenSSLProvider;

public class DataKeeperApplication extends Application<DataKeeperConfiguration> {
    static {
        Security.insertProviderAt(new OpenSSLProvider(), 1);
    }
    private static ZooKeeper zk;

    public static void main(final String[] args) throws Exception {
        DataKeeperClusterInfo clusterInfo = new DataKeeperClusterInfo("node1", "localhost", 2181);
        zk = new ZooKeeperConnection().connect("localhost", 2181, 10000);
        new DataKeeperApplication().run(args);
        new ZooKeeperClusterExecutor(zk, clusterInfo).run();
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
    public void run(final DataKeeperConfiguration configuration, final Environment environment) throws IOException {
        final DatabaseRepository kvs = new DatabaseRepository();
        final DatabaseResource dbResource = new DatabaseResource(kvs);
        final ClusterResource clusterResource = new ClusterResource(zk);
        final NodeResource nodeResource = new NodeResource();

        environment.jersey().register(dbResource);
        environment.jersey().register(clusterResource);
        environment.jersey().register(nodeResource);
    }

}
