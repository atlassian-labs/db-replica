package com.atlassian.db.replica.spi;

import java.sql.Connection;

public interface DataSource {
    Connection getConnection();
}
