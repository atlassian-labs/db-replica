package com.atlassian.db.replica.it.example.aurora.replica;

import com.atlassian.db.replica.api.SqlCall;
import com.atlassian.db.replica.it.example.aurora.replica.api.MultiReplicaConsistency;
import com.atlassian.db.replica.it.example.aurora.replica.api.SequenceReplicaConsistency;
import com.atlassian.db.replica.it.example.aurora.replica.api.SynchronousWriteConsistency;
import com.atlassian.db.replica.spi.ConnectionProvider;
import com.atlassian.db.replica.spi.ReplicaConsistency;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ConsistencyFactory {
    private final SqlCall<Connection> mainConnectionSupplier;
    private final ConnectionProvider connectionProvider; // TODO: IMO it's not a good dependency. Need to refactor later
    private final String readerEndpoint;
    private final String databaseName;

    public ConsistencyFactory(
        SqlCall<Connection> mainConnectionSupplier,
        ConnectionProvider connectionProvider,
        String readerEndpoint,
        String databaseName
    ) {
        this.mainConnectionSupplier = mainConnectionSupplier;
        this.connectionProvider = connectionProvider;
        this.readerEndpoint = readerEndpoint;
        this.databaseName = databaseName;
    }

    public ReplicaConsistency create() throws SQLException {
        initialize();
        final SequenceReplicaConsistency sequenceReplicaConsistency = new SequenceReplicaConsistency.Builder().build(
            "read_replica_replication"
        );
        final MultiReplicaConsistency multiReplicaConsistency = new MultiReplicaConsistency(
            sequenceReplicaConsistency,
            readerEndpoint,
            databaseName
        );
        return new SynchronousWriteConsistency(multiReplicaConsistency, connectionProvider, runnable -> {
        });
    }

    private void initialize() throws SQLException {
        try (final Connection connection = mainConnectionSupplier.call()) {
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
