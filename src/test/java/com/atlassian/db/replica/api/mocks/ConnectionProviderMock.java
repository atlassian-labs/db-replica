package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.ConnectionProvider;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectionProviderMock implements ConnectionProvider {
    private final boolean isAvailable;

    public ConnectionProviderMock() {
        this.isAvailable = true;
    }

    public ConnectionProviderMock(boolean isAvailable) {
        this.isAvailable = isAvailable;
    }

    public enum ConnectionType {
        MAIN,
        REPLICA
    }

    public List<ConnectionType> providedConnectionTypes = new ArrayList<>();
    public List<Connection> providedConnections = new ArrayList<>();
    public List<Statement> preparedStatements = new ArrayList<>();

    public List<Connection> getProvidedConnections() {
        return providedConnections;
    }

    public Connection singleProvidedConnection() {
        if (providedConnections.size() != 1) {
            throw new RuntimeException("Expected to provide a single connection");
        }
        return providedConnections.get(0);
    }

    public List<ConnectionType> getProvidedConnectionTypes() {
        return providedConnectionTypes;
    }

    public List<Statement> getPreparedStatements() {
        return preparedStatements;
    }

    @Override
    public boolean isReplicaAvailable() {
        return isAvailable;
    }

    @Override
    public Connection getMainConnection() {
        providedConnectionTypes.add(ConnectionType.MAIN);
        return getConnection();
    }

    @Override
    public Connection getReplicaConnection() {
        providedConnectionTypes.add(ConnectionType.REPLICA);
        return getConnection();
    }

    private Connection getConnection() {
        final Connection connection = mock(AutoCommitAwareConnection.class);
        try {
            doCallRealMethod().when(connection).setAutoCommit(anyBoolean());
            doCallRealMethod().when(connection).getAutoCommit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Mockito.when(connection.createStatement()).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(connection));
            Mockito.when(connection.createStatement(anyInt(), anyInt())).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(connection));
            Mockito.when(connection.createStatement(anyInt(), anyInt(), anyInt())).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(connection));
            Mockito.when(connection.prepareCall(anyString())).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(connection));
            Mockito.when(connection.prepareCall(
                anyString(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(connection));
            Mockito.when(connection.prepareCall(anyString(), anyInt(), anyInt(), anyInt())).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(connection));
            Mockito.when(connection.prepareStatement(anyString())).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
            Mockito.when(connection.prepareStatement(
                anyString(),
                any(int[].class)
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
            Mockito.when(connection.prepareStatement(
                anyString(),
                any(String[].class)
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(connection));
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        providedConnections.add(connection);
        return connection;
    }

    private PreparedStatement prepareStatement(Connection connection) throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(statement.executeQuery()).thenReturn(mock(ResultSet.class));
        initializeConnection(connection, statement);
        preparedStatements.add(statement);
        return statement;
    }

    private Statement createStatement(Connection connection) throws SQLException {
        final Statement statement = mock(Statement.class);
        initializeConnection(connection, statement);
        return statement;
    }

    private CallableStatement prepareCall(Connection connection) throws SQLException {
        final CallableStatement statement = mock(CallableStatement.class);
        initializeConnection(connection, statement);
        return statement;
    }

    private void initializeConnection(Connection connection, Statement statement) throws SQLException {
        when(statement.getConnection()).thenReturn(connection);
    }

    private static abstract class AutoCommitAwareConnection implements Connection {
        private Boolean isAutoCommitEnabled;

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            this.isAutoCommitEnabled = autoCommit;
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return isAutoCommitEnabled == null || isAutoCommitEnabled;
        }
    }
}
