package com.atlassian.db.replica.internal.aurora;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AuroraEndpointTest {

    @Test
    void shouldCreateAuroraEndpointFromReaderEndpoint() {
        // when
        AuroraEndpoint endpoint = AuroraEndpoint.parse("arch-app-staging-1-001-lr.cm9o6ayveq1a.us-east-9.rds.amazonaws.com");

        // then
        assertThat(endpoint.getServerId()).isEqualTo("arch-app-staging-1-001-lr");
        assertThat(endpoint.getCluster()).isEqualTo(AuroraCluster.AuroraClusterBuilder.anAuroraCluster("cm9o6ayveq1a").build());
        assertThat(endpoint.getDns()).isEqualTo(new RdsDns("us-east-9", "rds.amazonaws.com", null));
    }

}
