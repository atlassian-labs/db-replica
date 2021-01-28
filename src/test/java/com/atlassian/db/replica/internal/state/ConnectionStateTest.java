package com.atlassian.db.replica.internal.state;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.CircularConsistency;
import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.spi.state.StateListener;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.Queries.SELECT_FOR_UPDATE;
import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentInconsistency;
import static com.atlassian.db.replica.api.state.State.CLOSED;
import static com.atlassian.db.replica.api.state.State.MAIN;
import static com.atlassian.db.replica.api.state.State.NOT_INITIALISED;
import static com.atlassian.db.replica.api.state.State.COMMITED_MAIN;
import static com.atlassian.db.replica.api.state.State.REPLICA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ConnectionStateTest {

    private ConnectionProviderMock connectionProvider;
    private StateListener stateListener;

    @Before
    public void before() {
        this.stateListener = mock(StateListener.class);
        this.connectionProvider = new ConnectionProviderMock();
    }

    @Test
    public void shouldChangeStateFromNotInitialisedToReplicaForExecuteQuery() throws SQLException {
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
    public void shouldAvoidReplicaDueToInconsistency() throws SQLException {
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(NOT_INITIALISED, COMMITED_MAIN);
    }

    @Test
    public void shouldAvoidReplicaEvenIfAlreadyAcquiredDueToPossibleTransactions() throws SQLException {
        final Connection connection = DualConnection.builder(
            connectionProvider,
            new CircularConsistency.Builder(ImmutableList.of(true, false)).build()
        ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(REPLICA, COMMITED_MAIN);
    }

    @Test
    public void shouldForgiveReplicaIfItCatchesUpOnReads() throws SQLException {
        final Connection connection = DualConnection.builder(
            connectionProvider,
            new CircularConsistency.Builder(ImmutableList.of(false, true)).build()
        ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(COMMITED_MAIN, REPLICA);
    }

    @Test
    public void shouldAvoidReplicaWhenMainAlreadyAcquiredDueToPossibleTransactions() throws SQLException {
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).stateListener(stateListener)
            .build();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        Mockito.reset(stateListener);

        connection.prepareStatement(SIMPLE_QUERY).executeUpdate();

        verify(stateListener).transition(COMMITED_MAIN, MAIN);
    }

    @Test
    public void shouldReUseReadFromMainConnectionState() throws SQLException {
        final Connection connection = DualConnection.builder(
            connectionProvider,
            permanentInconsistency().build()
        ).stateListener(stateListener)
            .build();

        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();
        connection.prepareStatement(SIMPLE_QUERY).executeQuery();

        verify(stateListener).transition(NOT_INITIALISED, COMMITED_MAIN);
    }
}
