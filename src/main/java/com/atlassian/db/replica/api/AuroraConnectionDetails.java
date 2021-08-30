package com.atlassian.db.replica.api;

public final class AuroraConnectionDetails {

    private final String username;
    private final String password;

    private AuroraConnectionDetails(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static final class Builder {
        private String username;
        private String password;

        public static Builder anAuroraConnectionDetailsBuilder() {
            return new Builder();
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public AuroraConnectionDetails build() {
            return new AuroraConnectionDetails(username, password);
        }
    }

}
