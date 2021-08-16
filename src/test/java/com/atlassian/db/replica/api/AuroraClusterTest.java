package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.aurora.AuroraCluster;
import com.atlassian.db.replica.spi.DatabaseCluster;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;


class AuroraClusterTest {

    @Test
    public void shouldDiscoverReplicaConnectionsWhenScaledUp() throws SQLException {
        final AuroraClusterMock auroraClusterMock = new AuroraClusterMock();
        auroraClusterMock
            .scaleUp()
            .scaleUp();
        final DatabaseCluster auroraCluster = new AuroraCluster(auroraClusterMock::getMainConnection);

        Assertions.assertThat(auroraCluster.getReplicas().size()).isEqualTo(2);
    }

    @Test
    public void shouldDiscoverReplicaConnectionsWhenScaledDown() throws SQLException {
        final AuroraClusterMock auroraClusterMock = new AuroraClusterMock();
        auroraClusterMock
            .scaleUp()
            .scaleDown()
            .scaleUp();
        final DatabaseCluster auroraCluster = new AuroraCluster(auroraClusterMock::getMainConnection);

        Assertions.assertThat(auroraCluster.getReplicas().size()).isEqualTo(1);
    }
}
