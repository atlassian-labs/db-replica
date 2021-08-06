package com.atlassian.db.replica.spi;

import com.atlassian.db.replica.api.Database;

import java.sql.SQLException;
import java.util.Collection;

public interface DatabaseCluster {
    Collection<Database> getReplicas() throws SQLException;
}
