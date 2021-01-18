package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.spi.state.StateListener;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.Queries.SELECT_FOR_UPDATE;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentInconsistency;
import static com.atlassian.db.replica.api.state.State.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ConnectionStateTest {

    @Test
    public void shouldChangeStateFromNotInitialisedToReplicaForExecuteQuery() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(NOT_INITIALISED, REPLICA);
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToMainForExecuteUpdate() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(stateListener).transition(NOT_INITIALISED, MAIN);
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToMainWhenInconsistent() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(stateListener).transition(NOT_INITIALISED, MAIN);
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToMainForSelectForUpdate() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SELECT_FOR_UPDATE).executeQuery();

        verify(stateListener).transition(NOT_INITIALISED, MAIN);
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToReplicaAndFromReplicaToMain() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        verify(stateListener).transition(NOT_INITIALISED, REPLICA);
        Mockito.reset(stateListener);
        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();
        verify(stateListener).transition(REPLICA, MAIN);
    }

    @Test
    public void shouldChangeStateOnce() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(NOT_INITIALISED, REPLICA);
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToClosed() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.close();

        verify(stateListener).transition(NOT_INITIALISED, CLOSED);
    }

    @Test
    public void shouldChangeStateFromReplicaToClosed() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.close();

        verify(stateListener).transition(REPLICA, CLOSED);
    }

    @Test
    public void shouldChangeStateFromMainToClosed() throws SQLException {
        final StateListener stateListener = mock(StateListener.class);
        final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentConsistency().build()
        ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.close();

        verify(stateListener).transition(REPLICA, CLOSED);
    }

}
