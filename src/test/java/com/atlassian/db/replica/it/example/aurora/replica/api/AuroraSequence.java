package com.atlassian.db.replica.it.example.aurora.replica.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static java.lang.String.format;

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
            throw new RuntimeException(format("Can't bump sequence %s", sequenceName), e);
        }
    }

    public Long fetch(Connection connection) {
        try (PreparedStatement query = prepareFetchSequenceValueQuery(connection)) {
            query.setQueryTimeout(1);
            try (ResultSet results = query.executeQuery()) {
                results.next();
                return results.getLong("lsn");
            }
        } catch (Exception e) {
            throw new RuntimeException(format("error occurred during sequence[%s] value fetching", sequenceName), e);
        }
    }

    private PreparedStatement prepareBumpSequenceQuery(Connection connection) throws Exception {
        return connection.prepareStatement("UPDATE " + sequenceName + " SET lsn = lsn + 1 WHERE ID = (SELECT id FROM " + sequenceName + " FOR UPDATE SKIP LOCKED);");
    }

    private PreparedStatement prepareFetchSequenceValueQuery(Connection connection) throws Exception {
        return connection.prepareStatement("SELECT lsn FROM " + sequenceName + ";");
    }
}
