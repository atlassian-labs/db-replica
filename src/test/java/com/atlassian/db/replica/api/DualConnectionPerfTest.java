package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static org.assertj.core.api.Assertions.assertThat;

public class DualConnectionPerfTest {
    private final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldHaveAcceptableThruput() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final int times = 100000;

        final Duration duration = runBenchmark(connection, times);

        float thruputPerMillis = (float) times / duration.toMillis();
        float thruputPerSecond = thruputPerMillis * 1000;
        assertThat(thruputPerSecond)
            .as("thruput per second")
            .isGreaterThan(12_000);
    }

    private Duration runBenchmark(Connection connection, int times) throws SQLException {
        final Instant start = Instant.now();
        for (int i = 0; i < times; i++) {
            connection.prepareStatement(LARGE_SQL_QUERY).executeQuery().hashCode();
        }
        return Duration.between(start, Instant.now());
    }

}
