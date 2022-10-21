package com.atlassian.db.replica.internal.dispatcher;

import com.atlassian.db.replica.internal.RouteDecisionBuilder;
import com.atlassian.db.replica.internal.SqlFunction;
import com.atlassian.db.replica.internal.SqlQuery;
import com.atlassian.db.replica.internal.connection.state.ConnectionState;
import com.atlassian.db.replica.internal.connection.statements.operations.Operations;
import com.atlassian.db.replica.internal.observability.logs.LazyLogger;
import com.atlassian.db.replica.internal.util.concurrency.DecisionAwareReference;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.atlassian.db.replica.api.reason.Reason.LOCK;
import static com.atlassian.db.replica.api.reason.Reason.MAIN_CONNECTION_REUSE;
import static com.atlassian.db.replica.api.reason.Reason.RO_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.RW_API_CALL;
import static com.atlassian.db.replica.api.reason.Reason.WRITE_OPERATION;
import static com.atlassian.db.replica.internal.connection.state.State.MAIN;

public final class StatementDispatcher<S extends Statement> {
    private final ConnectionState state;
    private final LazyLogger logger;
    private final boolean compatibleWithPreviousVersion;
    private final SqlFunction sqlFunction;
    private final DecisionAwareReference<S> readStatement;
    private final DecisionAwareReference<S> writeStatement;
    private final Operations operations;
    private S currentStatement;


    public StatementDispatcher(
        ConnectionState state,
        LazyLogger logger,
        boolean compatibleWithPreviousVersion,
        Set<String> readOnlyFunctions,
        StatementCreator<S> statementCreator,
        Operations operations
    ) {
        this.state = state;
        this.logger = logger;
        this.compatibleWithPreviousVersion = compatibleWithPreviousVersion;
        this.sqlFunction = new SqlFunction(readOnlyFunctions);
        this.operations = operations;
        readStatement = new DecisionAwareReference<S>() {
            @Override
            public S create() throws Exception {
                return statementCreator.create(state.getReadConnection(getFirstCause()));
            }
        };
        writeStatement = new DecisionAwareReference<S>() {
            @Override
            public S create() throws Exception {
                return statementCreator.create(state.getWriteConnection(getFirstCause()));
            }
        };
    }

    public S getReadStatement(RouteDecisionBuilder decisionBuilder) throws SQLException {
        if (state.getState().equals(MAIN)) {
            decisionBuilder.reason(MAIN_CONNECTION_REUSE);
            state.getDecision().ifPresent(decisionBuilder::cause);
            logger.debug(() -> "Main connection reuse");
            return prepareWriteStatement(decisionBuilder);
        }
        String sql = decisionBuilder.getSql();
        if (sql != null) {
            SqlQuery sqlQuery = new SqlQuery(sql, compatibleWithPreviousVersion);
            if (sqlQuery.isWriteOperation(sqlFunction)) {
                decisionBuilder.reason(WRITE_OPERATION);
                logger.debug(() -> "write operation");
                return prepareWriteStatement(decisionBuilder);
            }
            if (sqlQuery.isSelectForUpdate()) {
                decisionBuilder.reason(LOCK);
                logger.debug(() -> "lock");
                return prepareWriteStatement(decisionBuilder);
            }
        }
        setCurrentStatement(getCurrentStatement() != null ? getCurrentStatement() : readStatement.get(decisionBuilder));
        operations.performOperations(getCurrentStatement());
        return getCurrentStatement();
    }

    public S getWriteStatement(RouteDecisionBuilder decisionBuilder) throws SQLException {
        return prepareWriteStatement(decisionBuilder);
    }

    public S getCurrentStatement() {
        return this.currentStatement;
    }

    public void close() {
        for (final Statement statement : allStatements()) {
            try {
                statement.close();
            } catch (Exception e) {
                // Ignore. We can't add it to warnings. It's impossible to read them after Statement#close
            }
        }
        readStatement.reset();
        writeStatement.reset();
        currentStatement = null;
    }

    private S prepareWriteStatement(RouteDecisionBuilder decisionBuilder) throws SQLException {
        setCurrentStatement(writeStatement.get(decisionBuilder));
        operations.performOperations(getCurrentStatement());
        return getCurrentStatement();
    }

    private void setCurrentStatement(S statement) {
        this.currentStatement = statement;
    }

    private Collection<Statement> allStatements() {
        final List<Statement> statements = new ArrayList<>();
        if (readStatement.isInitialized()) {
            statements.add(readStatement.get(new RouteDecisionBuilder(RO_API_CALL)));
        }
        if (writeStatement.isInitialized()) {
            statements.add(writeStatement.get(new RouteDecisionBuilder(RW_API_CALL)));
        }
        return statements;
    }
}
