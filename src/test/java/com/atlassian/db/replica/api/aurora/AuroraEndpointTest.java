package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.internal.aurora.AuroraEndpoint;
import com.atlassian.db.replica.internal.aurora.RdsDns;
import org.junit.jupiter.api.Test;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;
import static org.assertj.core.api.Assertions.assertThat;

class AuroraEndpointTest {
    @Test
    void shouldParseReaderEndpoint() {
        AuroraEndpoint endpoint = AuroraEndpoint.parse(
            "mydbcluster.cluster-ro-123456789012.us-east-1.rds.amazonaws.com:3306");

        assertThat(endpoint.getServerId()).isEqualTo("mydbcluster");
        assertThat(endpoint.getCluster()).isEqualTo(anAuroraCluster("123456789012").clusterPrefix("cluster-ro").build());
        assertThat(endpoint.getDns()).isEqualTo(new RdsDns("us-east-1", "rds.amazonaws.com", 3306));
    }

    @Test
    void shouldToString() {
        String original = "mydbcluster.cluster-ro-123456789012.us-east-1.rds.amazonaws.com:3306";
        String fromStringToString = AuroraEndpoint.parse(original).toString();

        assertThat(fromStringToString).isEqualTo(original);
    }
}
