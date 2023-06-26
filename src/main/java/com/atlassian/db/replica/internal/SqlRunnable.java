package com.atlassian.db.replica.internal;

import java.sql.SQLException;

public interface SqlRunnable {
    void run() throws SQLException;
}
