package com.atlassian.db.replica.it;

import com.atlassian.db.replica.api.DualConnection;
import com.atlassian.db.replica.internal.LsnReplicaConsistency;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.FETCH_REVERSE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReplicaStatementIT {

    @Test
    public void shouldImplementAllStatementMethods() throws SQLException {
        try (PostgresConnectionProvider connectionProvider = new PostgresConnectionProvider()) {
            final Connection connection = DualConnection.builder(
                connectionProvider,
                new LsnReplicaConsistency()
            ).build();
            final Statement statement = connection.createStatement();
            statement.executeQuery("SELECT 1;");
            statement.executeUpdate("CREATE SEQUENCE mysequence START 101;");
            statement.setMaxFieldSize(300);
            assertThat(statement.getMaxFieldSize()).isEqualTo(300);
            statement.setMaxRows(301);
            assertThat(statement.getMaxRows()).isEqualTo(301);
            statement.setEscapeProcessing(false);
            statement.setEscapeProcessing(true);
            statement.setQueryTimeout(33);
            assertThat(statement.getQueryTimeout()).isEqualTo(33);
            statement.getWarnings();
            statement.clearWarnings();
            statement.setCursorName("alosaf");
            statement.execute("SELECT 1;");
            statement.getResultSet();
            statement.getUpdateCount();
            statement.getMoreResults();
            statement.setFetchDirection(FETCH_REVERSE);
            assertThat(statement.getFetchDirection()).isEqualTo(FETCH_REVERSE);
            statement.setFetchSize(1234);
            assertThat(statement.getFetchSize()).isEqualTo(1234);
            statement.getResultSetConcurrency();
            statement.getResultSetType();
            statement.addBatch("SELECT 1;");
            statement.executeBatch();
            statement.clearBatch();
            statement.getConnection();
            statement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).getGeneratedKeys();
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).executeUpdate(
                "CREATE SEQUENCE mysequence2;",
                Statement.NO_GENERATED_KEYS
            );
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).executeUpdate(
                "CREATE SEQUENCE mysequence3;",
                new int[]{}
            );
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).executeUpdate(
                "CREATE SEQUENCE mysequence4;",
                new String[]{}
            );
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).execute(
                "CREATE SEQUENCE mysequence5;",
                RETURN_GENERATED_KEYS
            );
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).execute(
                "CREATE SEQUENCE mysequence6;",
                new int[]{}
            );
            connection.createStatement(
                TYPE_SCROLL_SENSITIVE,
                CONCUR_UPDATABLE
            ).execute(
                "CREATE SEQUENCE mysequence7;",
                new String[]{}
            );
            statement.getResultSetHoldability();
            statement.isClosed();
            statement.closeOnCompletion();
            statement.isCloseOnCompletion();
            statement.setLargeMaxRows(12345);
            assertThatThrownBy(statement::getLargeMaxRows).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(statement::executeLargeBatch).isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> statement.executeLargeUpdate("CREATE SEQUENCE mysequence2;"))
                .isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> statement.executeLargeUpdate("CREATE SEQUENCE mysequence2;", new int[]{}))
                .isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(() -> statement.executeLargeUpdate("CREATE SEQUENCE mysequence2;", new String[]{}))
                .isInstanceOf(SQLFeatureNotSupportedException.class);
            assertThatThrownBy(statement::cancel).isInstanceOf(SQLFeatureNotSupportedException.class);
            statement.close();
        }
    }
}
