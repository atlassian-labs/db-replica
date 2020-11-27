package com.atlassian.db.replica.api.mocks;

import java.sql.Connection;

public abstract class ReadOnlyAwareConnection implements Connection {
    private boolean isReadOnly;

    @Override
    public void setReadOnly(boolean readOnly) {
        this.isReadOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }
}
