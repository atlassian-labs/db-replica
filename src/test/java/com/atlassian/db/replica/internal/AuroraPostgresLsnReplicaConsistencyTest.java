package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.AuroraPostgresLsnReplicaConsistency;
import com.atlassian.db.replica.internal.util.ConnectionSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AuroraPostgresLsnReplicaConsistencyTest {

    private static final long LAST_WRITE_LSN = 8679792506L;

    private VolatileCache<Long> lastWriteCache;
    private AuroraPostgresLsnReplicaConsistency consistency;
    private Connection main;
    private Connection replica;

    @BeforeEach
    public void setUp() {
        lastWriteCache = new VolatileCache<>();
        consistency = new AuroraPostgresLsnReplicaConsistency.Builder()
            .cacheLastWrite(lastWriteCache)
            .build();

        main = mock(Connection.class);
        replica = mock(Connection.class);
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenLastWriteLsnFails() throws Exception {
        mockLsnFetchingFailure(main);

        assertThatThrownBy(() -> consistency.write(main)).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void shouldBeInconsistentWhenLastWriteLsnUnknown() {
        lastWriteCache.reset();

        boolean isConsistent = consistency.isConsistent(() -> new ConnectionSupplier(replica));

        assertThat(isConsistent).isFalse();
    }

    @Test
    public void shouldBeInconsistentWhenReplicaLsnFails() throws Exception {
        mockLsnFetching(main, LAST_WRITE_LSN);
        mockLsnFetchingFailure(replica);
        consistency.write(main);

        boolean isConsistent = consistency.isConsistent(() -> new ConnectionSupplier(replica));

        assertThat(isConsistent).isFalse();
    }

    @Test
    public void shouldBeInconsistentWhenReplicaIsBehind() throws Exception {
        mockLsnFetching(main, LAST_WRITE_LSN);
        mockLsnFetching(replica, LAST_WRITE_LSN - 1);
        consistency.write(main);

        boolean isConsistent = consistency.isConsistent(() -> new ConnectionSupplier(replica));

        assertThat(isConsistent).isFalse();
    }

    @Test
    public void shouldBeConsistentWhenReplicaIsAhead() throws Exception {
        mockLsnFetching(main, LAST_WRITE_LSN);
        mockLsnFetching(replica, LAST_WRITE_LSN + 1);
        consistency.write(main);

        boolean isConsistent = consistency.isConsistent(() -> new ConnectionSupplier(replica));

        assertThat(isConsistent).isTrue();
    }

    @Test
    public void shouldBeConsistentWhenReplicaCaughtUp() throws Exception {
        mockLsnFetching(main, LAST_WRITE_LSN);
        mockLsnFetching(replica, LAST_WRITE_LSN);
        consistency.write(main);

        boolean isConsistent = consistency.isConsistent(() -> new ConnectionSupplier(replica));

        assertThat(isConsistent).isTrue();
    }

    private void mockLsnFetching(Connection connection, long lsn) throws SQLException {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        ResultSet resultSet = mock(ResultSet.class);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.getLong("lsn")).thenReturn(lsn);
    }

    private void mockLsnFetchingFailure(Connection connection) throws SQLException {
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenThrow(new SQLException());
    }
}
