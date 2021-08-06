package com.atlassian.db.replica.internal.aurora;

import java.util.Objects;

import static com.atlassian.db.replica.internal.aurora.AuroraCluster.AuroraClusterBuilder.anAuroraCluster;

public final class AuroraCluster {
    private final String clusterName;
    private final String clusterPrefix;

    private AuroraCluster(String clusterName, String clusterPrefix) {
        this.clusterName = clusterName;
        this.clusterPrefix = clusterPrefix;
    }

    public static AuroraCluster parse(String cluster) {
        Objects.requireNonNull(cluster);

        String[] chunks = splitByLastOccurence("-", cluster);
        if (chunks.length == 1) {
            String clusterName = chunks[0];
            return anAuroraCluster(clusterName).build();
        } else {
            String clusterName = chunks[0];
            String clusterPrefix = chunks[1];
            return anAuroraCluster(clusterName).clusterPrefix(clusterPrefix).build();
        }
    }

    private static String[] splitByLastOccurence(String delimiter, String str) {
        int i = str.lastIndexOf(delimiter);
        if (i == -1) {
            return new String[]{str};
        } else {
            return new String[]{str.substring(i + 1), str.substring(0, i)};
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

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterPrefix() {
        return clusterPrefix;
    }


    public static final class AuroraClusterBuilder {
        private String clusterName;
        private String clusterPrefix;

        private AuroraClusterBuilder() {
        }

        public static AuroraClusterBuilder anAuroraCluster(String clusterName) {
            return new AuroraClusterBuilder().clusterName(clusterName);
        }

        public AuroraClusterBuilder clusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public AuroraClusterBuilder clusterPrefix(String clusterPrefix) {
            this.clusterPrefix = clusterPrefix;
            return this;
        }

        public AuroraCluster build() {
            return new AuroraCluster(clusterName, clusterPrefix);
        }
    }
}
