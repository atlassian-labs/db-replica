package com.atlassian.db.replica.it;

import com.atlassian.db.replica.spi.ConnectionProvider;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.Link;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Properties;

public class PostgresConnectionProvider implements ConnectionProvider, AutoCloseable {
    final DefaultDockerClientConfig config = DefaultDockerClientConfig
        .createDefaultConfigBuilder()
        .build();
    final DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
        .dockerHost(config.getDockerHost())
        .sslConfig(config.getSSLConfig())
        .build();
    final DockerClient dockerClient = DockerClientImpl.getInstance(config, httpClient);
    boolean isInitialized = false;

    @Override
    public boolean isReplicaAvailable() {
        return true;
    }

    @Override
    public Connection getMainConnection() {
        initialize();
        final String url = "jdbc:postgresql://localhost:5440/jira";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "jira");
        try {
            return DriverManager.getConnection(url, props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection getReplicaConnection() {
        initialize();
        final String url = "jdbc:postgresql://localhost:5450/jira";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "jira");
        try {
            return DriverManager.getConnection(url, props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void initialize() {
        if (isInitialized) {
            return;
        }
        isInitialized = true;
        cleanUp();
        pullImage();
        startMaster();
        startReplica();
    }

    private void pullImage() {
        try {
            dockerClient.pullImageCmd("bitnami/postgresql:13").start().awaitCompletion();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        cleanUp();
    }

    private void startReplica() {
        final CreateContainerResponse replicaCreate = dockerClient.createContainerCmd("bitnami/postgresql:13")
            .withEnv(
                "POSTGRESQL_REPLICATION_MODE=slave",
                "POSTGRESQL_MASTER_HOST=master",
                "POSTGRESQL_MASTER_PORT_NUMBER=5432",
                "POSTGRESQL_REPLICATION_USER=postgres",
                "POSTGRESQL_REPLICATION_PASSWORD=jira",
                "POSTGRESQL_PASSWORD=jira"
            )
            .withExposedPorts(ExposedPort.tcp(5432))
            .withPortBindings(PortBinding.parse("5450:5432"))
            .withName("db-replica-postgresql-replica")
            .withLinks(Link.parse("db-replica-postgresql-main:master"))
            .exec();
        dockerClient
            .startContainerCmd(replicaCreate.getId())
            .exec();
        for (int i = 0; i < 10; i++) {
            try {
                getReplicaConnection().close();
            } catch (Exception e) {
                try {
                    Thread.sleep(Duration.ofSeconds(1).toMillis());
                } catch (Exception ex) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void startMaster() {
        final CreateContainerResponse masterCreate = dockerClient.createContainerCmd("bitnami/postgresql:13")
            .withEnv(
                "POSTGRESQL_REPLICATION_MODE=master",
                "POSTGRESQL_USERNAME=postgres",
                "POSTGRESQL_PASSWORD=jira",
                "POSTGRESQL_DATABASE=jira",
                "POSTGRESQL_REPLICATION_USER=postgres",
                "POSTGRESQL_REPLICATION_PASSWORD=jira"
            ).withExposedPorts(ExposedPort.tcp(5432))
            .withPortBindings(PortBinding.parse("5440:5432"))
            .withName("db-replica-postgresql-main")
            .exec();
        dockerClient
            .startContainerCmd(masterCreate.getId())
            .exec();
        for (int i = 0; i < 10; i++) {
            try {
                getMainConnection().close();
            } catch (Exception e) {
                try {
                    Thread.sleep(Duration.ofSeconds(1).toMillis());
                } catch (Exception ex) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void cleanUp() {
        dockerClient
            .listContainersCmd()
            .withShowAll(true)
            .withNameFilter(
                ImmutableList.of(
                    "db-replica-postgresql-main",
                    "db-replica-postgresql-replica"
                )
            )
            .exec()
            .forEach(container -> {
                try {
                    dockerClient.stopContainerCmd(container.getId()).exec();
                } catch (Exception e) {
                    // it's probably already stopped
                }
                dockerClient.removeContainerCmd(container.getId()).exec();
            });
    }
}
