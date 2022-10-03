package com.atlassian.db.replica.api.mocks;

import com.atlassian.db.replica.spi.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

public class MockLogger implements Logger {
    private final List<String> messages = new LinkedList<>();

    public void debug(String message) {
        messages.add(message);
    }

    public void debug(String message, Throwable t) {
        messages.add(message);
    }

    public void info(String message) {
        messages.add(message);
    }

    public void info(String message, Throwable t) {
        messages.add(message);
    }

    public void warn(String message) {
        messages.add(message);
    }

    public void warn(String message, Throwable t) {
        messages.add(message);
    }

    public void error(String message) {
        messages.add(message);
    }

    public void error(String message, Throwable t) {
        messages.add(message);
    }

    public List<String> getMessages() {
        return messages;
    }

    @Override
    public String toString() {
        return messages.stream().map(message -> message + "\n").collect(joining());
    }
}
