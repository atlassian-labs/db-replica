package com.atlassian.db.replica.internal.aurora;

import java.util.Objects;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;

final class AuroraCluster {
    private final String clusterName;
    private final String clusterPrefix;

    private AuroraCluster(String clusterName, String clusterPrefix) {
        this.clusterName = clusterName;
        this.clusterPrefix = clusterPrefix;
    }

    static AuroraCluster parse(String cluster) {
        Objects.requireNonNull(cluster);

        String[] chunks = splitByLastOccurrence( cluster);
        String clusterName = chunks[0];
        if (chunks.length == 1) {
            return anAuroraCluster(clusterName).build();
        } else {
            String clusterPrefix = chunks[1];
            return anAuroraCluster(clusterName).clusterPrefix(clusterPrefix).build();
        }
    }

    @Override
    public String toString() {
        if (clusterPrefix != null && !clusterPrefix.isEmpty()) {
            return String.format("%s-%s", clusterPrefix, clusterName);
        } else {
            return clusterName;
        }
    }

    String getClusterName() {
        return clusterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AuroraCluster that = (AuroraCluster) o;

        if (!Objects.equals(clusterName, that.clusterName)) return false;
        return Objects.equals(clusterPrefix, that.clusterPrefix);
    }

    @Override
    public int hashCode() {
        int result = clusterName != null ? clusterName.hashCode() : 0;
        result = 31 * result + (clusterPrefix != null ? clusterPrefix.hashCode() : 0);
        return result;
    }

    private static String[] splitByLastOccurrence(String str) {
        int i = str.lastIndexOf("-");
        if (i == -1) {
            return new String[]{str};
        } else {
            return new String[]{str.substring(i + 1), str.substring(0, i)};
        }
    }
    static final class AuroraClusterBuilder {

        private String clusterName;
        private String clusterPrefix;

        private AuroraClusterBuilder() {
        }

        static AuroraClusterBuilder anAuroraCluster(String clusterName) {
            return new AuroraClusterBuilder().clusterName(clusterName);
        }

        AuroraClusterBuilder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        AuroraClusterBuilder clusterPrefix(String clusterPrefix) {
            this.clusterPrefix = clusterPrefix;
            return this;
        }

        AuroraCluster build() {
            return new AuroraCluster(clusterName, clusterPrefix);
        }
    }
}
