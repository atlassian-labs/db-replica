package com.atlassian.db.replica.internal;

import com.atlassian.db.replica.api.reason.RouteDecision;
import com.atlassian.db.replica.spi.Chief;

import java.sql.Connection;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.atlassian.db.replica.api.reason.Reason.WRITE_OPERATION;

public class DatabaseMicroManager implements Chief {
    final SqlFunction sqlFunction;

    public DatabaseMicroManager(Set<String> readOnlyFunctions) {
        this.sqlFunction = new SqlFunction(readOnlyFunctions);
    }

    @Override
    public void overrideDecision(
        RouteDecisionBuilder decisionBuilder,
        Collection<Supplier<Connection>> replicaConnections
    ) {
        final RouteDecision currentDecision = decisionBuilder.build();
        final Optional<String> sql = currentDecision.getSql();
        if (sql.isPresent()) {
            if (new SqlQuery(sql.get()).isWriteOperation(sqlFunction)) {
                decisionBuilder.reason(WRITE_OPERATION);
            }
        }
    }
}
