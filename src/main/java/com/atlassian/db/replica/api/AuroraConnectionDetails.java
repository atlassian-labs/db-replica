package com.atlassian.db.replica.api;

import com.atlassian.db.replica.spi.ReplicaConnectionPerUrlProvider;

import java.sql.DriverManager;

/**
 * @deprecated use {@link ReplicaConnectionPerUrlProvider} instead. example usage:
 * <pre>
 *     {@code
 *          url -> () -> DriverManager.getConnection(
 *              url.toString(),
 *              username,
 *              password
 *          )
 *     }
 * </pre>
 */
@Deprecated
public final class AuroraConnectionDetails {
    private final String username;
    private final String password;

    private AuroraConnectionDetails(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public ReplicaConnectionPerUrlProvider convert() {
        return replicaUrl -> () -> DriverManager.getConnection(
            replicaUrl.toString(),
            username,
            password
        );
    }

    /**
     * @deprecated see {@link AuroraConnectionDetails}.
     */
    @Deprecated
    public static final class Builder {
        private String username;
        private String password;

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        /**
         * @deprecated see {@link AuroraConnectionDetails}.
         */
        @Deprecated
        public static Builder anAuroraConnectionDetailsBuilder() {
            return new Builder();
        }

        public AuroraConnectionDetails build() {
            return new AuroraConnectionDetails(username, password);
        }
    }
}
