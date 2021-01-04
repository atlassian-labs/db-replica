package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectionWarnings {

    @Test
    public void shouldHaveNoWarningsByDefault() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        assertThat((Throwable) connection.getWarnings()).isNull();
    }

    @Test
    public void shouldGetWarningsFromMain() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat((Throwable) connection.getWarnings())
            .isEqualTo(mainWarning);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldGetWarningsFromReplica() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        assertThat((Throwable) connection.getWarnings())
            .isEqualTo(replicaWarning);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
    }

    @Test
    public void shouldPassWarningsBetweenConnections() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat((Throwable) connection.getWarnings())
            .isEqualTo(replicaWarning);
        assertThat((Throwable) connection.getWarnings().getNextWarning())
            .isEqualTo(mainWarning);
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA, MAIN);
    }

    @Test
    public void shouldClearMainWarnings() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        connection.clearWarnings();

        assertThat((Throwable) connection.getWarnings())
            .isNull();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
        Mockito.verify(connectionProvider.singleProvidedConnection()).clearWarnings();
    }

    @Test
    public void shouldClearReplicaWarnings() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        connection.clearWarnings();

        assertThat((Throwable) connection.getWarnings())
            .isNull();
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(REPLICA);
        Mockito.verify(connectionProvider.singleProvidedConnection()).clearWarnings();
    }

    @Test
    public void retrievalShouldBeIdempotent() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        for (int i = 0; i < 3; i++) {
            assertThat((Throwable) connection.getWarnings())
                .isEqualTo(mainWarning);
        }
        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsOnly(MAIN);
    }

    @Test
    public void shouldBreakWarningLoops() throws SQLException {
        final SQLWarning replicaWarning = new SQLWarning("Replica warning");
        replicaWarning.setNextWarning(replicaWarning);
        final SQLWarning mainWarning = new SQLWarning("Main warning");
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock(
            true,
            mainWarning,
            replicaWarning
        );
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        assertThat((Throwable) connection.getWarnings())
            .isEqualTo(replicaWarning);
    }

}
