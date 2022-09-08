package com.atlassian.db.replica.spi;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * The hook will be invoked before closing a connection with uncommitted data.
 */
public interface DirtyConnectionCloseHook {
    void onClose(Connection connection) throws SQLException;
}
