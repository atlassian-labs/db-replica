package com.atlassian.db.replica.api;

import java.sql.SQLException;

public interface SqlRun {
    void run() throws SQLException;
}
