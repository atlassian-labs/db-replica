package com.atlassian.db.replica.api;

import java.sql.Connection;

public interface Database {
    String getUuid();

    SqlCall<Connection> getConnection();
}
