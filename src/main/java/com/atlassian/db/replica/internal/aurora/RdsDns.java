package com.atlassian.db.replica.internal.aurora;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class RdsDns {
    private static final Pattern DNS_PATTERN = Pattern.compile("([^.]+).([^:]*):?(\\d+)?");
    private final String region;
    private final String domain;
    private final Integer port;

    public RdsDns(String region, String domain, Integer port) {
        this.region = region;
        this.domain = domain;
        this.port = port;
    }

     static RdsDns parse(String dns) {
        Objects.requireNonNull(dns);

        Matcher matcher = DNS_PATTERN.matcher(dns);
        matcher.matches();
        return new RdsDns(matcher.group(1), matcher.group(2), parseNullableInteger(groupOrNull(matcher)));
    }

    private static String groupOrNull(Matcher matcher) {
        try {
            return matcher.group(3);
        } catch (IndexOutOfBoundsException e) {
            return null;
        }
    }

    private static Integer parseNullableInteger(String str) {
        if (str == null) {
            return null;
        } else {
            return Integer.valueOf(str);
        }
    }

    @Override
    public String toString() {
        if (port != null) {
            return String.format("%s.%s:%s", region, domain, port);
        } else {
            return String.format("%s.%s", region, domain);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RdsDns rdsDns = (RdsDns) o;

        if (!Objects.equals(region, rdsDns.region)) return false;
        if (!Objects.equals(domain, rdsDns.domain)) return false;
        return Objects.equals(port, rdsDns.port);
    }

    @Override
    public int hashCode() {
        int result = region != null ? region.hashCode() : 0;
        result = 31 * result + (domain != null ? domain.hashCode() : 0);
        result = 31 * result + (port != null ? port.hashCode() : 0);
        return result;
    }
}
