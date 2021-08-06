package com.atlassian.db.replica.it.example.aurora.app;

import com.atlassian.db.replica.api.SqlCall;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public class Users {
    private final SqlCall<Connection> connectionSupplier;

    public Users(SqlCall<Connection> connectionSupplier) throws SQLException {
        this.connectionSupplier = connectionSupplier;
        initialize();
    }

    public void add(User user) throws SQLException {
        try (final Connection dualConnection = connectionSupplier.call()) {
            insertNewUser(dualConnection, user.getName());
        }
    }

    public Collection<User> fetch() throws SQLException {
        try (final Connection connection = connectionSupplier.call()) {
            final ImmutableList.Builder<User> usersBuilder = ImmutableList.builder();
            final PreparedStatement preparedStatement = connection.prepareStatement(
                "SELECT username FROM users");
            final ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                final String username = resultSet.getString(1);
                usersBuilder.add(new User(username));
            }
            return usersBuilder.build();
        }
    }

    private void initialize() throws SQLException {
        try (final Connection connection = connectionSupplier.call()) {
            try (final PreparedStatement preparedStatement = connection.prepareStatement(
                "CREATE TABLE IF NOT EXISTS users (username VARCHAR ( 50 ));")) {
                preparedStatement.executeUpdate();
            }
        }
    }

    private void insertNewUser(Connection writerConnection, String newUesrName) throws SQLException {
        try (final PreparedStatement preparedStatement = writerConnection.prepareStatement(
            "INSERT INTO users VALUES(?)")) {
            preparedStatement.setString(1, newUesrName);
            preparedStatement.executeUpdate();
        }
    }
}
