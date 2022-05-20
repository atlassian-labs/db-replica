package com.atlassian.db.replica.api;

import com.atlassian.db.replica.spi.DataSource;

import java.util.Optional;

public interface Database {
    default Optional<String> getId() {
        return Optional.empty();
    }

    DataSource getDataSource();
}
