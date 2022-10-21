package com.atlassian.db.replica.internal.connection.statements.operations;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class Operations {
    @SuppressWarnings("rawtypes")
    private final List<Operation> operationList = new ArrayList<>();

    public void add(@SuppressWarnings("rawtypes") Operation operation) {
        operationList.add(operation);
    }

    public void clear() {
        operationList.clear();
    }

    public void remove(Object object){
        //noinspection SuspiciousMethodCalls
        operationList.remove(object);
    }
    public void performOperations(Statement statement) throws SQLException {
        //noinspection rawtypes
        for (Operation operation : operationList) {
            //noinspection unchecked
            operation.accept(statement);
        }
        operationList.clear();
    }

}
