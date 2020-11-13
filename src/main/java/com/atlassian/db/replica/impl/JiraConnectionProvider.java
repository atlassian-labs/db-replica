package com.atlassian.db.replica.impl;

import com.atlassian.db.replica.spi.*;
import com.atlassian.jira.ofbiz.sql.*;
import com.atlassian.jira.replica.internal.*;
import org.ofbiz.core.entity.config.*;

import java.sql.*;

public class JiraConnectionProvider implements ConnectionProvider {
    final String replicaDataSource = "replicaDS1";
    private final String helperName;
    private final TransactionFactoryInterfaceWrapper transactionFactoryInterfaceWrapper;

    public JiraConnectionProvider(
        String helperName,
        TransactionFactoryInterfaceWrapper transactionFactoryInterfaceWrapper
    ) {
        this.helperName = helperName;
        this.transactionFactoryInterfaceWrapper = transactionFactoryInterfaceWrapper;
    }

    @Override
    public boolean isReplicaAvailable() {
        return EntityConfigUtil.getInstance().getDatasourceInfo(replicaDataSource) != null;
    }

    @Override
    public Connection getMainConnection() {
        try {
            return transactionFactoryInterfaceWrapper.getRealConnection(helperName);
        } catch (Exception e) {
            throw new ReadReplicaUnsupportedOperationException(e);
        }
    }

    @Override
    public Connection getReplicaConnection() {
        try {
            return transactionFactoryInterfaceWrapper.getRealConnection(replicaDataSource);
        } catch (Exception e) {
            throw new ReadReplicaUnsupportedOperationException(e);
        }
    }

}
