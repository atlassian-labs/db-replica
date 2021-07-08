package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.internal.aurora.AuroraCluster;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class AuroraClusterTest {

    @Test
    void parseClusterWithoutPrefix() {
        AuroraCluster cluster = AuroraCluster.parse("232938923");
        assertNull(cluster.getClusterPrefix());
        assertThat(cluster.getClusterName()).isEqualTo("232938923");
    }

    @Test
    void parseCluster() {
        AuroraCluster cluster = AuroraCluster.parse("cluster-pr-232938923");
        assertThat(cluster.getClusterPrefix()).isEqualTo("cluster-pr");
        assertThat(cluster.getClusterName()).isEqualTo("232938923");
    }
}
