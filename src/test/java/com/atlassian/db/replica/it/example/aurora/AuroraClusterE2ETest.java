package com.atlassian.db.replica.it.example.aurora;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.it.example.aurora.app.User;
import com.atlassian.db.replica.it.example.aurora.app.Users;
import com.atlassian.db.replica.it.example.aurora.replica.AuroraConnectionProvider;
import com.atlassian.db.replica.it.example.aurora.replica.ConsistencyFactory;
import com.atlassian.db.replica.it.example.aurora.utils.DecisionLog;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.atlassian.db.replica.api.AuroraConnectionDetails.Builder.anAuroraConnectionDetailsBuilder;
import static com.atlassian.db.replica.api.reason.Reason.READ_OPERATION;
import static com.atlassian.db.replica.api.reason.Reason.REPLICA_INCONSISTENT;
import static org.assertj.core.api.Assertions.assertThat;

class AuroraClusterE2ETest {
    final String readerJdbcUrl = System.getenv("PG_AURORA_TESTS_DB_URL_READ");
    final String writerJdbcUrl = System.getenv("PG_AURORA_TESTS_DB_URL");

    @Test
    void shouldUtilizeReplicaForReadQueriesForSynchronisedWrites() throws SQLException {
        final DecisionLog decisionLog = new DecisionLog();
        final SqlCall<Connection> connectionPool = initializeConnectionPool(decisionLog);
        final Users users = new Users(connectionPool);
        final User newUser = new User();

        users.add(newUser);
        final Collection<User> allUsers = users.fetch();

        final List<Reason> reasons = decisionLog.getDecisions().stream().map(RouteDecision::getReason).collect(
            Collectors.toList());
        assertThat(allUsers).contains(newUser);
        assertThat(decisionLog.getDecisions()).contains(new RouteDecision(
            "SELECT username FROM users",
            READ_OPERATION,
            null
        ));

        assertThat(reasons).isNotEmpty().doesNotContain(REPLICA_INCONSISTENT);
    }

    private SqlCall<Connection> initializeConnectionPool(final DatabaseCall decisionLog) throws SQLException {
        final ConnectionProvider connectionProvider = new AuroraConnectionProvider(
            readerJdbcUrl,
            writerJdbcUrl
        );

        AuroraConnectionDetails auroraConnectionDetails = anAuroraConnectionDetailsBuilder()
            .build();

        final ReplicaConsistency replicaConsistency = new ConsistencyFactory(
            connectionProvider,
            auroraConnectionDetails
        ).create();

        return () -> DualConnection.builder(
                connectionProvider,
                replicaConsistency
            ).databaseCall(decisionLog)
            .build();
    }

}
