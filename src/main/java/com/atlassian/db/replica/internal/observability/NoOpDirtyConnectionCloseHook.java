package com.atlassian.db.replica.internal.observability;

import com.atlassian.db.replica.spi.DirtyConnectionCloseHook;

import java.sql.Connection;

public class NoOpDirtyConnectionCloseHook implements DirtyConnectionCloseHook {
    @Override
    public void onClose(Connection connection) {
        // do nothing
    }
}
