package com.atlassian.db.replica.internal.aurora;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class AuroraEndpoint {
    private static final String SERVER_ID_PATTERN = "([^.]+)";
    private static final String CLUSTER_PATTERN = "([^.]+)";
    private static final String DNS_PATTERN = "(.*)";
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile(SERVER_ID_PATTERN + "." + CLUSTER_PATTERN + "." + DNS_PATTERN);

    private final String serverId;
    private final AuroraCluster cluster;
    private final RdsDns dns;

    AuroraEndpoint(String serverId, AuroraCluster cluster, RdsDns dns) {
        this.serverId = serverId;
        this.cluster = cluster;
        this.dns = dns;
    }

    static AuroraEndpoint parse(String readerEndpoint) {
        Objects.requireNonNull(readerEndpoint);

        Matcher matcher = ENDPOINT_PATTERN.matcher(readerEndpoint);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format("Can't parse %s.", readerEndpoint));
        }
        return new AuroraEndpoint(
            matcher.group(1),
            AuroraCluster.parse(matcher.group(2)),
            RdsDns.parse(matcher.group(3))
        );
    }

    String getServerId() {
        return serverId;
    }

    AuroraCluster getCluster() {
        return cluster;
    }

    RdsDns getDns() {
        return dns;
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", serverId, cluster.toString(), dns.toString());
    }


    static final class AuroraEndpointBuilder {
        private String serverId;
        private AuroraCluster cluster;
        private RdsDns dns;

        private AuroraEndpointBuilder() {
        }

        static AuroraEndpointBuilder anAuroraEndpoint(AuroraEndpoint endpoint) {
            return new AuroraEndpointBuilder()
                .serverId(endpoint.serverId)
                .cluster(endpoint.cluster)
                .dns(endpoint.dns);
        }

        static AuroraEndpointBuilder anAuroraEndpoint() {
            return new AuroraEndpointBuilder();
        }

        AuroraEndpointBuilder serverId(String serverId) {
            this.serverId = serverId;
            return this;
        }

        AuroraEndpointBuilder cluster(AuroraCluster cluster) {
            this.cluster = cluster;
            return this;
        }

        AuroraEndpointBuilder dns(RdsDns dns) {
            this.dns = dns;
            return this;
        }

        AuroraEndpoint build() {
            return new AuroraEndpoint(serverId, cluster, dns);
        }
    }
}

