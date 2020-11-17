package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.Queries.SIMPLE_QUERY;

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
                Mockito.verify(st).close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
