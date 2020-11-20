package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@SuppressWarnings("ThrowableNotThrown")
public class TestStatement {
    private final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldCloseAllStatements() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();
        statement.executeUpdate();

        statement.close();

        connectionProvider.getPreparedStatements().forEach(st -> {
            try {
                verify(st).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void shouldHaveNoWarningsByDefault() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final SQLWarning warnings = statement.getWarnings();

        assertThat((Throwable) warnings).isNull();
    }

    @Test
    public void shouldGetWarningsOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getWarnings();

        verify(connectionProvider.singleStatement()).getWarnings();
    }

    @Test
    public void shouldGetWarningsOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getWarnings();

        verify(connectionProvider.singleStatement()).getWarnings();
    }

    @Test
    public void shouldClearWarningsOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.clearWarnings();

        verify(connectionProvider.singleStatement()).clearWarnings();
    }

    @Test
    public void shouldClearWarningsOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.clearWarnings();

        verify(connectionProvider.singleStatement()).clearWarnings();
    }

    @Test
    public void shouldGetResultSetOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getResultSet();

        verify(connectionProvider.singleStatement()).getResultSet();
    }

    @Test
    public void shouldGetResultSetOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getResultSet();

        verify(connectionProvider.singleStatement()).getResultSet();
    }

    @Test
    public void shouldGetNullResultSetBeforeQueryExecuted() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final ResultSet resultSet = statement.getResultSet();

        assertThat(resultSet).isNull();
    }

    @Test
    public void shouldGetUpdateCountOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getUpdateCount();

        verify(connectionProvider.singleStatement()).getUpdateCount();
    }

    @Test
    public void shouldGetUpdateCountOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getUpdateCount();

        verify(connectionProvider.singleStatement()).getUpdateCount();
    }

    @Test
    public void shouldGetMinusOneForGetUpdateCountBeforeQueryExecuted() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final int updateCount = statement.getUpdateCount();

        assertThat(updateCount).isEqualTo(-1);
    }

    @Test
    public void shouldGetMoreResultsOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getMoreResults();

        verify(connectionProvider.singleStatement()).getMoreResults();
    }

    @Test
    public void shouldGetMoreResultsOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getMoreResults();

        verify(connectionProvider.singleStatement()).getMoreResults();
    }

    @Test
    public void shouldNotHaveMoreResultsBeforeQueryExecuted() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final boolean hasMoreResults = statement.getMoreResults();

        assertThat(hasMoreResults).isEqualTo(false);
    }

    @Test
    public void shouldSetFetchSizeOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setFetchSize(10);
        statement.executeUpdate();


        verify(connectionProvider.singleStatement()).setFetchSize(10);
    }

    @Test
    public void shouldSetFetchSizeOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setFetchSize(10);
        statement.executeQuery();


        verify(connectionProvider.singleStatement()).setFetchSize(10);
    }
}