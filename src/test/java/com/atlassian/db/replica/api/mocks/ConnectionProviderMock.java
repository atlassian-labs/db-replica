package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.ConnectionProvider;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("MagicConstant")
public class ConnectionProviderMock implements ConnectionProvider {
    private final boolean isAvailable;
    private final SQLWarning mainWarning;
    private final SQLWarning replicaWarning;

    public ConnectionProviderMock() {
        this.isAvailable = true;
        this.mainWarning = null;
        this.replicaWarning = null;
    }

    public ConnectionProviderMock(boolean isAvailable) {
        this.isAvailable = isAvailable;
        this.mainWarning = null;
        this.replicaWarning = null;
    }

    public ConnectionProviderMock(boolean isAvailable, SQLWarning mainWarning, SQLWarning replicaWarning) {
        this.isAvailable = isAvailable;
        this.mainWarning = mainWarning;
        this.replicaWarning = replicaWarning;
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

    public Statement singleStatement() {
        if (preparedStatements.size() != 1) {
            throw new RuntimeException("Expected to provide a single statement");
        }
        return preparedStatements.get(0);
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
        final Connection connection = getConnection();
        setWarning(connection, mainWarning);
        return connection;
    }

    @Override
    public Connection getReplicaConnection() {
        providedConnectionTypes.add(ConnectionType.REPLICA);
        final Connection connection = getConnection();
        setWarning(connection, replicaWarning);
        return connection;
    }

    private void setWarning(Connection connection, SQLWarning warning) {
        try {
            final Warning warn = new Warning(warning);
            //noinspection ThrowableNotThrown
            doAnswer(invocation -> warn.getWarning()).when(connection).getWarnings();
            doAnswer(invocation -> {
                warn.clear();
                return null;
            }).when(connection).clearWarnings();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConnection() {
        final Connection connection = mock(AutoCommitAwareConnection.class);
        try {
            doCallRealMethod().when(connection).setAutoCommit(anyBoolean());
            doCallRealMethod().when(connection).getAutoCommit();
            doCallRealMethod().when(connection).close();
            doCallRealMethod().when(connection).isClosed();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Mockito.when(connection.createStatement()).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(
                connection));
            Mockito.when(connection.createStatement(
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(connection));
            Mockito.when(connection.createStatement(
                anyInt(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<Statement>) invocationOnMock -> createStatement(connection));
            Mockito.when(connection.prepareCall(anyString())).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(
                connection));
            Mockito.when(connection.prepareCall(
                anyString(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(connection));
            Mockito.when(connection.prepareCall(
                anyString(),
                anyInt(),
                anyInt(),
                anyInt()
            )).thenAnswer((Answer<CallableStatement>) invocationOnMock -> prepareCall(connection));
            Mockito.when(connection.prepareStatement(anyString())).thenAnswer((Answer<PreparedStatement>) invocationOnMock -> prepareStatement(
                connection));
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

    private static class Warning {
        private SQLWarning warning;

        public Warning(SQLWarning warning) {
            this.warning = warning;
        }

        public SQLWarning getWarning() {
            return warning;
        }

        public void clear() {
            this.warning = null;
        }
    }

    private static abstract class AutoCommitAwareConnection implements Connection {
        private Boolean isAutoCommitEnabled;
        private Boolean isClosed;

        @Override
        public void setAutoCommit(boolean autoCommit) {
            this.isAutoCommitEnabled = autoCommit;
        }

        @Override
        public boolean getAutoCommit() {
            return isAutoCommitEnabled == null || isAutoCommitEnabled;
        }

        @Override
        public void close() throws SQLException {
            this.isClosed = true;
        }

        @Override
        public boolean isClosed() {
            return isClosed != null ;
        }
    }
}
