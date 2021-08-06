package com.atlassian.db.replica.internal.aurora;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class AuroraEndpoint {
    private static final Pattern ENDPOINT_PATTERN = Pattern.compile("([^.]+).([^.]+).(.*)");
    private final String serverId;
    private final AuroraCluster cluster;
    private final RdsDns dns;

    public AuroraEndpoint(String serverId, AuroraCluster cluster, RdsDns dns) {
        this.serverId = serverId;
        this.cluster = cluster;
        this.dns = dns;
    }

    public static AuroraEndpoint parse(String readerEndpoint) {
        Objects.requireNonNull(readerEndpoint);

        Matcher matcher = ENDPOINT_PATTERN.matcher(readerEndpoint);
        matcher.matches();
        return new AuroraEndpoint(
            matcher.group(1),
            AuroraCluster.parse(matcher.group(2)),
            RdsDns.parse(matcher.group(3))
        );
    }

    public String getServerId() {
        return serverId;
    }

    public AuroraCluster getCluster() {
        return cluster;
    }

    public RdsDns getDns() {
        return dns;
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", serverId, cluster.toString(), dns.toString());
    }


    public static final class AuroraEndpointBuilder {
        private String serverId;
        private AuroraCluster cluster;
        private RdsDns dns;

        private AuroraEndpointBuilder() {
        }

        public static AuroraEndpointBuilder anAuroraEndpoint(AuroraEndpoint endpoint) {
            return new AuroraEndpointBuilder()
                .serverId(endpoint.serverId)
                .cluster(endpoint.cluster)
                .dns(endpoint.dns);
        }

        public static AuroraEndpointBuilder anAuroraEndpoint() {
            return new AuroraEndpointBuilder();
        }

        public AuroraEndpointBuilder serverId(String serverId) {
            this.serverId = serverId;
            return this;
        }

        public AuroraEndpointBuilder cluster(AuroraCluster cluster) {
            this.cluster = cluster;
            return this;
        }

        public AuroraEndpointBuilder dns(RdsDns dns) {
            this.dns = dns;
            return this;
        }

        public AuroraEndpoint build() {
            return new AuroraEndpoint(serverId, cluster, dns);
        }
    }
}

