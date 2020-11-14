package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.*;
import org.junit.*;
import org.mockito.*;

import java.sql.*;

public class TestStatement {
    public static final String QUERY = "SELECT 1;";
    private ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldCloseAllStatements() throws SQLException {
        final DualConnection connection = new DualConnection(connectionProvider, new PermanentConsistency());
        final PreparedStatement statement = connection.prepareStatement(QUERY);
        statement.executeQuery();
        statement.executeUpdate();

        statement.close();

        connectionProvider.getPreparedStatements().forEach(st -> {
            try {
                Mockito.verify(st).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
