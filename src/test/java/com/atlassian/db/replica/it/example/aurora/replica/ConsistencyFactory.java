package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.api.AuroraMultiReplicaConsistency;
import com.atlassian.db.replica.it.example.aurora.replica.api.SequenceReplicaConsistency;
import com.atlassian.db.replica.it.example.aurora.replica.api.SynchronousWriteConsistency;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ConsistencyFactory {
    private final ConnectionProvider connectionProvider;
    private final ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider;

    public ConsistencyFactory(
        ConnectionProvider connectionProvider,
        ReplicaConnectionPerUrlProvider replicaConnectionPerUrlProvider
    ) {
        this.connectionProvider = connectionProvider;
        this.replicaConnectionPerUrlProvider = replicaConnectionPerUrlProvider;
    }

    public ReplicaConsistency create() throws SQLException {
        initialize();
        final SequenceReplicaConsistency sequenceReplicaConsistency = SequenceReplicaConsistency.builder()
            .sequenceName("read_replica_replication")
            .build();
        final ReplicaConsistency multiReplicaConsistency = AuroraMultiReplicaConsistency.builder()
            .replicaConsistency(sequenceReplicaConsistency)
            .replicaConnectionPerUrlProvider(replicaConnectionPerUrlProvider)
            .build();
        return new SynchronousWriteConsistency(multiReplicaConsistency, connectionProvider);
    }

    private void initialize() throws SQLException {
        try (final Connection connection = connectionProvider.getMainConnection()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS read_replica_replication\n" +
                    "    (\n" +
                    "        id integer PRIMARY KEY,\n" +
                    "        lsn bigint NOT NULL\n" +
                    "    );")) {
                preparedStatement.executeUpdate();
            }
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                " INSERT INTO read_replica_replication (id, lsn)\n" +
                    "        SELECT 1, 1 WHERE 1 NOT IN (SELECT id FROM read_replica_replication);")) {
                preparedStatement.executeUpdate();
            }
        }
    }
}
