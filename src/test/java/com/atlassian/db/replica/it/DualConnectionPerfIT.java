package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.mocks.NoOpConnectionProvider;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

import static com.atlassian.db.replica.api.Queries.LARGE_SQL_QUERY;
import static com.atlassian.db.replica.api.mocks.CircularConsistency.permanentConsistency;
import static org.assertj.core.api.Assertions.assertThat;

public class DualConnectionPerfIT {

    @Test
    public void shouldHaveAcceptableThruput() throws SQLException {
        final Connection connection = DualConnection
            .builder(
                new NoOpConnectionProvider(),
                permanentConsistency().build()
            ).build();
        final int times = 100000000;

        final Duration duration = runBenchmark(connection, times);

        float thruputPerMillis = (float) times / duration.toMillis();
        assertThat(thruputPerMillis)
            .as("thruput per ms")
            .isGreaterThan(3_000);
    }

    private Duration runBenchmark(Connection connection, int times) throws SQLException {
        final Instant start = Instant.now();
        int hashCode = 0;
        for (int i = 0; i < times; i++) {
            hashCode += connection.prepareStatement(LARGE_SQL_QUERY).executeQuery().hashCode();
        }
        System.out.println("I really need that number. JIT gods don't kill my code paths. " + hashCode);
        return Duration.between(start, Instant.now());
    }

}
