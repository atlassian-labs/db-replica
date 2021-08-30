package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.api.AuroraConnectionDetails;
import com.atlassian.db.replica.it.example.aurora.replica.api.SequenceReplicaConsistency;
import com.atlassian.db.replica.it.example.aurora.replica.api.SynchronousWriteConsistency;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.atlassian.db.replica.api.AuroraMultiReplicaConsistency.Builder.anAuroraMultiReplicaConsistencyBuilder;

public class ConsistencyFactory {
    private final ConnectionProvider connectionProvider;
    private final AuroraConnectionDetails auroraConnectionDetails;

    public ConsistencyFactory(
        ConnectionProvider connectionProvider,
        AuroraConnectionDetails auroraConnectionDetails
    ) {
        this.connectionProvider = connectionProvider;
        this.auroraConnectionDetails = auroraConnectionDetails;
    }

    public ReplicaConsistency create() throws SQLException {
        initialize();
        final SequenceReplicaConsistency sequenceReplicaConsistency = new SequenceReplicaConsistency.Builder().build(
            "read_replica_replication"
        );
        final ReplicaConsistency multiReplicaConsistency = anAuroraMultiReplicaConsistencyBuilder()
            .replicaConsistency(sequenceReplicaConsistency)
            .auroraConnectionDetails(auroraConnectionDetails)
            .build();
        return new SynchronousWriteConsistency(multiReplicaConsistency, connectionProvider, runnable -> { });
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
