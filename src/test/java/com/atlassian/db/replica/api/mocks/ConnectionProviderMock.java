package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.*;
import org.mockito.*;
import org.mockito.stubbing.*;

import java.sql.*;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
        final Connection connection = mock(Connection.class);
        try {
            Mockito.when(connection.createStatement()).thenReturn(mock(Statement.class));
            Mockito.when(connection.createStatement(anyInt(), anyInt())).thenReturn(mock(Statement.class));
            Mockito.when(connection.createStatement(anyInt(), anyInt(), anyInt())).thenReturn(mock(Statement.class));
            Mockito.when(connection.prepareCall(anyString())).thenReturn(mock(CallableStatement.class));
            Mockito.when(connection.prepareCall(
                anyString(),
                anyInt(),
                anyInt()
            )).thenReturn(mock(CallableStatement.class));
            Mockito.when(connection.prepareCall(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mock(
                CallableStatement.class));
            Mockito.when(connection.prepareStatement(anyString())).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
            Mockito.when(connection.prepareStatement(
                anyString(),
                anyInt(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
            Mockito.when(connection.prepareStatement(
                anyString(),
                any(int[].class)
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
            Mockito.when(connection.prepareStatement(
                anyString(),
                any(String[].class)
            )).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> getStatement());
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
        providedConnections.add(connection);
        return connection;
    }

    private PreparedStatement getStatement() {
        final PreparedStatement statement = mock(PreparedStatement.class);
        preparedStatements.add(statement);
        return statement;
    }
}
