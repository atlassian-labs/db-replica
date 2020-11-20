package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.never;
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

    @Test
    public void shouldAddBatchOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.executeUpdate();

        verify(connectionProvider.singleStatement()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldAddBatchOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldClearBatchOnMain() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.addBatch(SIMPLE_QUERY);
        statement.clearBatch();
        statement.executeUpdate();

        verify(connectionProvider.singleStatement(), never()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldClearBatchOnReplica() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.addBatch(SIMPLE_QUERY);
        statement.clearBatch();
        statement.executeQuery();

        verify(connectionProvider.singleStatement(), never()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldUnwrapStatement() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);

        final Statement statement = preparedStatement.unwrap(Statement.class);

        assertThat(statement).isEqualTo(preparedStatement);
    }

    @Test
    public void shouldFailUnwrapWithSqlException() {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);

        Throwable thrown = catchThrowable(() -> preparedStatement.unwrap(Integer.class));

        assertThat(thrown).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldCheckIfIsWrappedForStatement() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);
        preparedStatement.executeQuery();

        final boolean isWrappedFor = preparedStatement.isWrapperFor(Statement.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldCheckIfIsWrappedForInteger() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);
        preparedStatement.executeQuery();

        final boolean isWrappedFor = preparedStatement.isWrapperFor(Integer.class);

        assertThat(isWrappedFor).isFalse();
    }

    @Test
    public void shouldNotBeClosedBeforeUse() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final Statement statement = connection.prepareStatement(SIMPLE_QUERY);

        final boolean isClosed = statement.isClosed();

        assertThat(isClosed).isFalse();
    }

    @Test
    public void shouldClose() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final Statement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery(SIMPLE_QUERY);

        statement.close();
        statement.isClosed();

        verify(connectionProvider.singleStatement()).close();
        verify(connectionProvider.singleStatement()).isClosed();
    }
}