package com.atlassian.db.replica.it.example.aurora.replica.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class AuroraSequence {
    private final String sequenceName;

    public AuroraSequence(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void tryBump(Connection connection) {
        try (
            PreparedStatement query = prepareBumpSequenceQuery(connection)
        ) {
            query.executeUpdate();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (Exception e) {
//            LOG.withoutCustomerData().error("error occurred during bumping sequence {}", sequenceName, e);
            throw new RuntimeException(e);
        }
    }

    public Long fetch(Connection connection) {
        try (PreparedStatement query = prepareFetchSequenceValueQuery(connection)) {
            query.setQueryTimeout(1);  //TODO parametrise
            try (ResultSet results = query.executeQuery()) {
                results.next();
                final long lsn = results.getLong("lsn");
                return lsn;
            }
        } catch (Exception e) {
//            LOG.withoutCustomerData().error("error occurred during sequence[{}] value fetching", sequenceName, e);
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement prepareBumpSequenceQuery(Connection connection) throws Exception {
        return connection.prepareStatement("UPDATE " + sequenceName + " SET lsn = lsn + 1 WHERE ID = (SELECT id FROM " + sequenceName + " FOR UPDATE SKIP LOCKED);");
    }

    private PreparedStatement prepareFetchSequenceValueQuery(Connection connection) throws Exception {
        return connection.prepareStatement("SELECT lsn FROM " + sequenceName + ";");
    }
}
