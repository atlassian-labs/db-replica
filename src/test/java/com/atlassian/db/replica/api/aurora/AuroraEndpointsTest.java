package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.internal.aurora.AuroraEndpoints;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class AuroraEndpointsTest {
    @Test
    void shouldTransformClusterEndpointToInstanceEndpoint() {
        String instanceEndpoint = AuroraEndpoints.instanceEndpoint(
            "mydbcluster.cluster-ro-123456789012.us-east-1.rds.amazonaws.com:3306",
            "mydbcluster-lr"
        ).toString();

        assertThat(instanceEndpoint).isEqualTo("mydbcluster-lr.123456789012.us-east-1.rds.amazonaws.com:3306");
    }
}
