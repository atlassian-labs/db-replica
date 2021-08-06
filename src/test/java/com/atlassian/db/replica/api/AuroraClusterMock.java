package com.atlassian.db.replica.api;

import com.google.common.collect.Sets;
import org.h2.tools.SimpleResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class AuroraClusterMock {
    private final Connection connection = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");
    private static final Set<Node> replicas = Sets.newConcurrentHashSet();
    private static final AtomicInteger counter = new AtomicInteger();

    public AuroraClusterMock() throws SQLException {
        counter.set(0);
        replicas.clear();
        try (Statement statement = getMainConnection().createStatement()) {
            //noinspection SqlNoDataSourceInspection
            statement.executeUpdate(
                "CREATE ALIAS IF NOT EXISTS aurora_global_db_instance_status FOR \"com.atlassian.db.replica.api.AuroraClusterMock.auroraGlobalDbInstanceStatus\";");
        }
    }

    public Connection getMainConnection() {
        return connection;
    }

    public AuroraClusterMock scaleUp() {
        replicas.add(
            new Node(
                "apg-global-db-rpo-mammothrw-elephantro-n" + counter.incrementAndGet(),
                UUID.randomUUID().toString()
            )
        );
        return this;
    }

    public AuroraClusterMock scaleDown() {
        replicas.remove(replicas.stream().findAny().orElse(null));
        return this;
    }

    //example:
    //server_id                                   |              session_id              | aws_region | durable_lsn | highest_lsn_rcvd | feedback_epoch | feedback_xmin | oldest_read_view_lsn | visibility_lag_in_msec
    //--------------------------------------------+--------------------------------------+------------+-------------+------------------+----------------+---------------+----------------------+------------------------
    //apg-global-db-rpo-mammothrw-elephantro-1-n1 | MASTER_SESSION_ID                    | us-east-1  | 93763985102 |                  |                |               |                      |
    //apg-global-db-rpo-mammothrw-elephantro-1-n2 | f38430cf-6576-479a-b296-dc06b1b1964a | us-east-1  | 93763985099 |      93763985102 |              2 |    3315479243 |          93763985095 |                     10
    //apg-global-db-rpo-elephantro-mammothrw-n1   | 0d9f1d98-04ad-4aa4-8fdd-e08674cbbbfe | us-west-2  | 93763985095 |      93763985099 |              2 |    3315479243 |          93763985089 |                   1017
    //(3 rows)
    public static ResultSet auroraGlobalDbInstanceStatus() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("SERVER_ID", Types.VARCHAR, 255, 0);
        rs.addColumn("SESSION_ID", Types.VARCHAR, 255, 0);
        rs.addRow("apg-global-db-rpo-mammothrw-elephantro-1-n1", "MASTER_SESSION_ID");
        replicas.forEach(replica -> rs.addRow(replica.getServerId(), replica.getSessionId()));
        return rs;
    }

    private static class Node {
        private final String serverId;
        private final String sessionId;

        public Node(String serverId, String sessionId) {
            this.serverId = serverId;
            this.sessionId = sessionId;
        }

        public String getServerId() {
            return serverId;
        }

        public String getSessionId() {
            return sessionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(serverId, node.serverId) && Objects.equals(sessionId, node.sessionId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(serverId, sessionId);
        }

        @Override
        public String toString() {
            return "Node{" +
                "serverId='" + serverId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                '}';
        }
    }
}
