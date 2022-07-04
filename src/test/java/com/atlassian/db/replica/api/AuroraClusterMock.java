package com.atlassian.db.replica.api;

import com.google.common.collect.Sets;
import org.h2.tools.SimpleResultSet;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuroraClusterMock {
    private final Connection connection;
    private static final Set<Node> replicas = Sets.newConcurrentHashSet();
    private static final AtomicInteger counter = new AtomicInteger();

    public AuroraClusterMock() throws SQLException {
        counter.set(0);
        replicas.clear();
        this.connection = mock(Connection.class);
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(this.connection.prepareStatement(eq("SELECT server_id FROM aurora_replica_status() WHERE session_id != 'MASTER_SESSION_ID' and last_update_timestamp > NOW() - INTERVAL '5 minutes'")))
            .thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenAnswer((Answer<ResultSet>) invocation -> {
            return auroraGlobalDbInstanceStatus();
        });
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

    public static ResultSet auroraGlobalDbInstanceStatus() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("SERVER_ID", Types.VARCHAR, 255, 0);
        rs.addColumn("SESSION_ID", Types.VARCHAR, 255, 0);
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
