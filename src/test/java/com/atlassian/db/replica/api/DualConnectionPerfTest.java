package com.atlassian.db.replica.api;

import com.atlassian.db.replica.api.mocks.ConnectionProviderMock;
import com.atlassian.db.replica.api.mocks.PermanentConsistency;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;

public class DualConnectionPerfTest {
    private final ConnectionProviderMock connectionProvider = new ConnectionProviderMock();

    @Test
    public void shouldHaveAcceptableThroughput() throws SQLException {
        final DualConnection connection = DualConnection.builder(connectionProvider, new PermanentConsistency()).build();
        final int times = 100000;
        final Duration duration = runBenchmark(connection, times);
        final int runsPerSecond = (int) (((times + 0.0f) / duration.toMillis()) * 100);

        Assertions.assertThat(runsPerSecond).isGreaterThan(1800);
    }

    private Duration runBenchmark(Connection connection, int times) throws SQLException {
        final Instant start = Instant.now();
        for (int i = 0; i < times; i++) {
            connection.prepareStatement(LARGE_SQL_QUERY).executeQuery().hashCode();
        }
        return Duration.between(start, Instant.now());
    }

}
