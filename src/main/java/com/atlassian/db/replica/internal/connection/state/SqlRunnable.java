package com.atlassian.db.replica.internal.connection.state;

import java.sql.SQLException;

interface SqlRunnable {
    void run() throws SQLException;
}
