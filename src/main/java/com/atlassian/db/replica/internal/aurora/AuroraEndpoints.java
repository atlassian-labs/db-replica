package com.atlassian.db.replica.internal.aurora;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;
import static com.atlassian.db.replica.internal.aurora.AuroraEndpoint.AuroraEndpointBuilder.anAuroraEndpoint;

public class AuroraEndpoints {
    private AuroraEndpoints() {
    }

    /**
     * Transforms reader endpoint to instance endpoint
     */
    public static AuroraEndpoint instanceEndpoint(AuroraEndpoint readerEndpoint, String serverId) {
        return anAuroraEndpoint(readerEndpoint)
            .serverId(serverId)
            .cluster(anAuroraCluster(readerEndpoint.getCluster().getClusterName()).clusterPrefix(null).build())
            .build();
    }
}
