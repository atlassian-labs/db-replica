package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import com.atlassian.db.replica.api.mocks.PermanentInconsistency;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.MAIN;
import static com.atlassian.db.replica.api.mocks.ConnectionProviderMock.ConnectionType.REPLICA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;

public class TestTransactions {
    public static final String QUERY = "SELECT 1;";
    private ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldUseReadConnectionsForExecuteQueryInTransaction() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        startTransaction(connection);
        connection.prepareStatement(QUERY).executeQuery();
        connection.commit();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA);
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        Mockito.verify(replica).setAutoCommit(false);
    }

    @Test
    public void shouldUseTransactionForBothReadAndWriteConnections() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        startTransaction(connection);
        connection.prepareStatement(QUERY).executeQuery();
        connection.prepareStatement(QUERY).execute();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        Mockito.verify(replica).setAutoCommit(false);
        Mockito.verify(main).setAutoCommit(false);
    }

    @Test
    public void shouldUseTransactionForWriteConnection() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(QUERY).executeQuery();
        startTransaction(connection);
        connection.prepareStatement(QUERY).execute();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        Mockito.verify(replica, never()).setAutoCommit(false);
        Mockito.verify(main).setAutoCommit(false);
    }

    @Test
    public void shouldNotStartTransactionIfNeverUsed() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        connection.prepareStatement(QUERY).executeQuery();
        connection.prepareStatement(QUERY).execute();
        startTransaction(connection);

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        Mockito.verify(replica, never()).setAutoCommit(anyBoolean());
        Mockito.verify(main, never()).setAutoCommit(anyBoolean());
    }

    @Test
    public void shouldShouldUseTransactionForTheSameStatement() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        final PreparedStatement preparedStatement = connection.prepareStatement(QUERY);
        preparedStatement.executeQuery();
        startTransaction(connection);
        preparedStatement.execute();


        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(REPLICA, MAIN);
        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        Mockito.verify(replica, never()).setAutoCommit(false);
        Mockito.verify(main).setAutoCommit(false);
    }

    @Test
    public void shouldRollbackMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        final PreparedStatement preparedStatement = connection.prepareStatement(QUERY);
        startTransaction(connection);
        preparedStatement.execute();
        connection.rollback();

        final Connection main = connectionProvider.getProvidedConnections().get(0);
        Mockito.verify(main).rollback();
    }

    @Test
    public void shouldRollbackReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        final PreparedStatement preparedStatement = connection.prepareStatement(QUERY);
        startTransaction(connection);
        preparedStatement.executeQuery();
        connection.rollback();

        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        Mockito.verify(replica).rollback();
    }

    @Test
    public void shouldCommitTransactions() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();

        final PreparedStatement preparedStatement = connection.prepareStatement(QUERY);
        startTransaction(connection);
        preparedStatement.executeQuery();
        connection.commit();
        preparedStatement.execute();
        connection.commit();

        final Connection replica = connectionProvider.getProvidedConnections().get(0);
        final Connection main = connectionProvider.getProvidedConnections().get(1);
        Mockito.verify(replica).commit();
        Mockito.verify(main).commit();
    }

    @Test
    public void shouldUseMainForRepeatableReadTransactionIsolationLevel() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        connection.prepareStatement(QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    @Test
    public void shouldUseMainForSerializableTransactionIsolationLevel() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentInconsistency()).build();
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        connection.prepareStatement(QUERY).executeQuery();

        assertThat(connectionProvider.getProvidedConnectionTypes())
            .containsExactly(MAIN);
    }

    private void startTransaction(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
}
