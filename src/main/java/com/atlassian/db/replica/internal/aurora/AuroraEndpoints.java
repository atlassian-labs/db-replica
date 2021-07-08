package com.atlassian.db.replica.internal.aurora;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;
import static com.atlassian.db.replica.internal.aurora.AuroraEndpoint.AuroraEndpointBuilder.anAuroraEndpoint;

public class AuroraEndpoints {
    private AuroraEndpoints() {
    }

    /**
     * Transforms reader endpoint to instance endpoint
     */
    public static AuroraEndpoint instanceEndpoint(String readerEndpoint, String serverId) {
        AuroraEndpoint endpoint = AuroraEndpoint.parse(readerEndpoint);
        return anAuroraEndpoint(endpoint)
            .serverId(serverId)
            .cluster(anAuroraCluster(endpoint.getCluster().getClusterName()).clusterPrefix(null).build())
            .build();
    }
}
