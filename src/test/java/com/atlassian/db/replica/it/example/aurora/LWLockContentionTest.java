package com.atlassian.db.replica.it.example.aurora;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class LWLockContentionTest {
    final String url = "jdbc:postgresql://database-1.cluster-c2vqiwlre1di.eu-central-1.rds.amazonaws.com:5432/postgres";
    final String user = "postgres";
    final String password = System.getenv("password");
    final String PROJECT_TABLE_NAME = "project";
    final String ISSUE_TABLE_NAME = "jiraissue";
    final AtomicInteger selectRuns = new AtomicInteger();
    final AtomicLong duration = new AtomicLong();

    @Test
    void shouldCauseLWLocContention() throws SQLException, InterruptedException {
        cleanup();
        createDatabaseTables();
        final AtomicBoolean continueBenchmark = new AtomicBoolean(true);
        final CountDownLatch longRunningThreadLatch = new CountDownLatch(1);


        startLongRunningTransaction(longRunningThreadLatch);
        createIssuesConcurrently(16, 10000);
        System.out.println("Finished creating issues: " + Instant.now());
        Thread.sleep(Duration.ofMinutes(5).toMillis());
        selectProjectsBenchmark(continueBenchmark,16);
        Thread.sleep(Duration.ofMinutes(5).toMillis());
        longRunningThreadLatch.countDown();
        Thread.sleep(Duration.ofMinutes(5).toMillis());

        continueBenchmark.set(false);
        System.out.printf("Run %d in %d ms%n", selectRuns.get(), duration.get());
    }

    private void startLongRunningTransaction(CountDownLatch latch) {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.submit(() -> {
            try (final Connection connection = getConnection()) {
                connection.setAutoCommit(false);
                try (final Statement statement = connection.createStatement()) {
                    statement.execute("INSERT INTO " + PROJECT_TABLE_NAME + "(id,pcounter) VALUES(" + 100 + ",1);");
                    latch.await();
                    connection.commit();
                }

            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        executorService.shutdown();
    }

    private void selectProjectsBenchmark(AtomicBoolean continueBenchmark, int concurrencyLevel) throws SQLException {
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
        for (int i = 0; i < concurrencyLevel; i++) {
            executorService.submit(() -> {
                try (final Connection connection = getConnection()) {
                    while (continueBenchmark.get()) {
                        try (final Statement statement = connection.createStatement()) {
                            final Instant now = Instant.now();
                            try (final ResultSet resultSet = statement.executeQuery("SELECT * FROM " + PROJECT_TABLE_NAME + ";")) {
                                if (!resultSet.next()) {
                                    throw new RuntimeException("No results");
                                }
                                duration.addAndGet(Duration.between(now, Instant.now()).toMillis());
                                selectRuns.incrementAndGet();
                            }
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
    }

    private void createIssuesConcurrently(int concurrencyLevel, int issues) {
        final AtomicInteger remainingIssues = new AtomicInteger(issues);
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLevel);
        final ArrayList<Future> futures = new ArrayList<Future>();
        for (int i = 0; i < concurrencyLevel; i++) {
            final int projectId = i;
            final Future<?> future = executorService.submit(() -> {
                try (Connection projectConnection = getConnection()) {
                    try (Connection issueCreateConnection = getConnection()) {
                        while (remainingIssues.getAndDecrement() > 0) {
                            createIssue(issueCreateConnection, projectConnection, projectId);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            futures.add(future);
        }
        futures.stream().forEach(future -> {
            try {
                future.get(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.shutdown();
    }

    private void createIssue(Connection issueCreateConnection, Connection projectBumpConnection, int projectId) throws SQLException {
        issueCreateConnection.setAutoCommit(false);
        projectBumpConnection.setAutoCommit(false);
        try (final Statement statement = projectBumpConnection.createStatement()) {
            statement.execute("UPDATE " + PROJECT_TABLE_NAME + " SET pcounter = pcounter + 1 WHERE id = " + projectId + ";");
        }
        try (final Statement statement = issueCreateConnection.createStatement()) {
            statement.execute("INSERT INTO " + ISSUE_TABLE_NAME + "(project,version) VALUES(" + projectId + ",1);");
        }
        issueCreateConnection.commit();
        projectBumpConnection.commit();

    }

    private void createDatabaseTables() throws SQLException {
        try (Connection connection = getConnection()) {
            try (final Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + PROJECT_TABLE_NAME + "\n" +
                        "(\n" +
                        "    id                       bigint PRIMARY KEY,\n" +
                        "    pcounter                 bigint);");
                for (int i = 0; i < 100; i++) {
                    statement.execute("INSERT INTO " + PROJECT_TABLE_NAME + "(id,pcounter) VALUES(" + i + ",1);");
                }

                statement.execute("CREATE TABLE " + ISSUE_TABLE_NAME + "\n" +
                        "(\n" +
                        "    id                       SERIAL PRIMARY KEY,\n" +
                        "    project                       bigint references " + PROJECT_TABLE_NAME + "(id),\n" +
                        "    version                 bigint);");
            }
        }
    }

    private void cleanup() throws SQLException {
        try (Connection connection = getConnection()) {
            try (final Statement statement = connection.createStatement()) {
                statement.execute("drop table if exists " + ISSUE_TABLE_NAME + ";");
                statement.execute("drop table if exists " + PROJECT_TABLE_NAME + ";");
            }
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(
                url,
                user,
                password
        );
    }

}
