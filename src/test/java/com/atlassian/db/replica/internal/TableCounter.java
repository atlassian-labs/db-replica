package com.atlassian.db.replica.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.function.Supplier;

import static java.lang.String.format;

public class TableCounter implements DatabaseProgress<Long> {

    private final String table;

    public TableCounter(String table) {
        this.table = table;
    }

    @Override
    public Long updateMain(Supplier<Connection> main) {
        Connection mainConnection = main.get();
        try (PreparedStatement update = bump(mainConnection)) {
            update.executeUpdate();
            return fetchSequenceValue(mainConnection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement bump(Connection connection) throws Exception {
        return connection.prepareStatement(
            "UPDATE " + table + " SET lsn = lsn + 1"
                + " WHERE ID = (SELECT id FROM " + table + " FOR UPDATE SKIP LOCKED);"
        );
    }

    private Long fetchSequenceValue(Connection connection) {
        try (
            PreparedStatement select = fetch(connection);
            ResultSet results = select.executeQuery()
        ) {
            results.next();
            return results.getLong("lsn");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private PreparedStatement fetch(Connection connection) throws Exception {
        return connection.prepareStatement("SELECT lsn FROM " + table + ";");
    }

    @Override
    public Long getMain(Supplier<Connection> main) {
        return fetchSequenceValue(main.get());
    }

    @Override
    public Long getReplica(Supplier<Connection> replica) {
        return fetchSequenceValue(replica.get());
    }

    public void initialize(Connection connection) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                format("CREATE TABLE IF NOT EXISTS %s(id integer PRIMARY KEY, lsn bigint NOT NULL);", table)
            );
            statement.execute(
                format("INSERT INTO %s (id, lsn) SELECT 1, 0 WHERE 1 NOT IN (SELECT id FROM %s);", table, table)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
