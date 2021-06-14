package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SuppressWarnings("ThrowableNotThrown")
public class TestStatement {

    @Test
    public void shouldCloseAllStatements() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();
        statement.executeUpdate();
        connectionProvider.getPreparedStatements().forEach(st -> {
            try {
                doThrow(new RuntimeException()).when(st).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
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
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final SQLWarning warnings = statement.getWarnings();

        assertThat((Throwable) warnings).isNull();
    }

    @Test
    public void shouldGetWarningsOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getWarnings();

        verify(connectionProvider.singleStatement()).getWarnings();
    }

    @Test
    public void shouldGetWarningsOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getWarnings();

        verify(connectionProvider.singleStatement()).getWarnings();
    }

    @Test
    public void shouldClearWarningsOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.clearWarnings();

        verify(connectionProvider.singleStatement()).clearWarnings();
    }

    @Test
    public void shouldClearWarningsOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.clearWarnings();

        verify(connectionProvider.singleStatement()).clearWarnings();
    }

    @Test
    public void shouldGetResultSetOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getResultSet();

        verify(connectionProvider.singleStatement()).getResultSet();
    }

    @Test
    public void shouldGetResultSetOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getResultSet();

        verify(connectionProvider.singleStatement()).getResultSet();
    }

    @Test
    public void shouldGetNullResultSetBeforeQueryExecuted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final ResultSet resultSet = statement.getResultSet();

        assertThat(resultSet).isNull();
    }

    @Test
    public void shouldGetUpdateCountOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getUpdateCount();

        verify(connectionProvider.singleStatement()).getUpdateCount();
    }

    @Test
    public void shouldGetUpdateCountOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getUpdateCount();

        verify(connectionProvider.singleStatement()).getUpdateCount();
    }

    @Test
    public void shouldGetMinusOneForGetUpdateCountBeforeQueryExecuted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final int updateCount = statement.getUpdateCount();

        assertThat(updateCount).isEqualTo(-1);
    }

    @Test
    public void shouldGetMoreResultsOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeUpdate();

        statement.getMoreResults();

        verify(connectionProvider.singleStatement()).getMoreResults();
    }

    @Test
    public void shouldGetMoreResultsOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.getMoreResults();

        verify(connectionProvider.singleStatement()).getMoreResults();
    }

    @Test
    public void shouldNotHaveMoreResultsBeforeQueryExecuted() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        final boolean hasMoreResults = statement.getMoreResults();

        assertThat(hasMoreResults).isFalse();
    }

    @Test
    public void shouldSetFetchSizeOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setFetchSize(10);
        statement.executeUpdate();

        verify(connectionProvider.singleStatement()).setFetchSize(10);
    }

    @Test
    public void shouldSetFetchSizeOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setFetchSize(10);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setFetchSize(10);
    }

    @Test
    public void shouldAddBatchOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.executeUpdate();

        verify(connectionProvider.singleStatement()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldAddBatchOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldClearBatchOnMain() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.addBatch(SIMPLE_QUERY);
        statement.clearBatch();
        statement.executeUpdate();

        verify(connectionProvider.singleStatement(), never()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldClearBatchOnReplica() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.addBatch(SIMPLE_QUERY);
        statement.addBatch(SIMPLE_QUERY);
        statement.clearBatch();
        statement.executeQuery();

        verify(connectionProvider.singleStatement(), never()).addBatch(SIMPLE_QUERY);
    }

    @Test
    public void shouldUnwrapStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection
            .builder(connectionProvider, permanentConsistency().build())
            .build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);

        final Statement statement = preparedStatement.unwrap(Statement.class);

        assertThat(statement).isEqualTo(preparedStatement);
    }

    @Test
    public void shouldFailUnwrapWithSqlException() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);

        Throwable thrown = catchThrowable(() -> preparedStatement.unwrap(Integer.class));

        assertThat(thrown).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldCheckIfIsWrappedForStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);
        preparedStatement.executeQuery();

        final boolean isWrappedFor = preparedStatement.isWrapperFor(Statement.class);

        assertThat(isWrappedFor).isTrue();
    }

    @Test
    public void shouldCheckIfIsWrappedForInteger() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement preparedStatement = connection.prepareStatement(SIMPLE_QUERY);
        preparedStatement.executeQuery();

        final boolean isWrappedFor = preparedStatement.isWrapperFor(Integer.class);

        assertThat(isWrappedFor).isFalse();
    }

    @Test
    public void shouldNotBeClosedBeforeUse() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Statement statement = connection.prepareStatement(SIMPLE_QUERY);

        final boolean isClosed = statement.isClosed();

        assertThat(isClosed).isFalse();
    }

    @Test
    public void shouldClose() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final Statement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery(SIMPLE_QUERY);

        statement.close();

        assertThat(statement.isClosed()).isTrue();
        verify(connectionProvider.singleStatement()).close();
    }

    @Test
    public void shouldSetEscapeProcessing() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setEscapeProcessing(true);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setEscapeProcessing(true);
    }

    @Test
    public void shouldSetMaxRows() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setMaxRows(123);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setMaxRows(123);
    }

    @Test
    public void shouldSetMaxFieldSize() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setMaxFieldSize(123);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setMaxFieldSize(123);
    }

    @Test
    public void shouldSetFetchDirection() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setFetchDirection(ResultSet.FETCH_FORWARD);
    }

    @Test
    public void shouldSetPoolable() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setPoolable(false);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setPoolable(false);
    }

    @Test
    public void shouldSetLargeMaxRows() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.setLargeMaxRows(12);
        statement.executeQuery();

        verify(connectionProvider.singleStatement()).setLargeMaxRows(12);
    }

    @Test
    public void shouldShouldNotDelegateToClosedStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.executeQuery();

        statement.close();
        final Throwable thrown = catchThrowable(statement::getResultSetHoldability);

        verify(connectionProvider.singleStatement(), never()).getResultSetHoldability();
        assertThat(thrown).isNotNull();
    }


    @Test
    public void shouldCloseStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);

        statement.close();

        assertThat(statement.isClosed()).isTrue();
    }

    @Test
    public void shouldNotReuseClosedStatement() throws SQLException {
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).build();
        final PreparedStatement statement = connection.prepareStatement(SIMPLE_QUERY);
        statement.close();

        final Throwable thrown = catchThrowable(statement::executeQuery);

        assertThat(thrown).isInstanceOf(SQLException.class);
    }
}
