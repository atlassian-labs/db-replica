package com.atlassian.db.replica.it.example.aurora;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.api.aurora.AuroraCluster;
import com.atlassian.db.replica.api.aurora.ReplicaNode;
import com.atlassian.db.replica.api.reason.Reason;
import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.it.example.aurora.app.User;
import com.atlassian.db.replica.it.example.aurora.app.Users;
import com.atlassian.db.replica.it.example.aurora.replica.AuroraConnectionProvider;
import com.atlassian.db.replica.it.example.aurora.replica.ConsistencyFactory;
import com.atlassian.db.replica.it.example.aurora.replica.api.ReplicaNodeAwareConnectionProvider;
import com.atlassian.db.replica.it.example.aurora.utils.DecisionLog;
import com.atlassian.db.replica.it.example.aurora.utils.ReplicationLag;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.DatabaseCall;
import com.atlassian.db.replica.spi.DatabaseCluster;
import com.atlassian.db.replica.spi.ReplicaConsistency;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.atlassian.db.replica.api.reason.Reason.READ_OPERATION;
import static com.atlassian.db.replica.api.reason.Reason.REPLICA_INCONSISTENT;
import static org.assertj.core.api.Assertions.assertThat;

public class AuroraClusterTest {
    final String writerEndpoint = "jdbc:postgresql://database-1.cluster-crmnlihjxqlm.eu-central-1.rds.amazonaws.com:5432/newdb";
    final String readerEndpoint = "jdbc:postgresql://database-1.cluster-ro-crmnlihjxqlm.eu-central-1.rds.amazonaws.com:5432/newdb";

    //TODO simplify API
    @Test
    @Ignore
    public void shouldUtilizeReplicaForReadQueriesForSynchronisedWrites() throws SQLException {
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
        assertThat(reasons).doesNotContain(REPLICA_INCONSISTENT);
    }

    private SqlCall<Connection> initializeConnectionPool(final DatabaseCall decisionLog) throws SQLException {
        final ReplicaNode replicaNode = new ReplicaNode();
        final ConnectionProvider connectionProvider = new AuroraConnectionProvider(
            readerEndpoint,
            writerEndpoint
        );
        final ReplicaNodeAwareConnectionProvider multiReplicaConnectionProvider = new ReplicaNodeAwareConnectionProvider(
            connectionProvider
        );
        final DatabaseCluster cluster = new AuroraCluster(connectionProvider::getMainConnection, replicaNode);
        final ReplicaConsistency replicaConsistency = new ConsistencyFactory(
            connectionProvider::getMainConnection,
            connectionProvider,
            cluster
        ).create();
        final SqlCall<Connection> connectionPool = () -> DualConnection.builder(
            multiReplicaConnectionProvider,
            replicaConsistency
        ).databaseCall(decisionLog)
            .build();
        new ReplicationLag(connectionPool).set(10);
        return connectionPool;
    }

}
