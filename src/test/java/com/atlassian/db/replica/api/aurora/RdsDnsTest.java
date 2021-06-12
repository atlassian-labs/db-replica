package com.atlassian.db.replica.api.aurora;

import com.atlassian.db.replica.internal.aurora.RdsDns;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;


class RdsDnsTest {

    @Test
    void parseRdsDns() {
        RdsDns dns = RdsDns.parse("us-east-1.rds.amazonaws.com:3306");

        assertThat(dns.getRegion()).isEqualTo("us-east-1");
        assertThat(dns.getDomain()).isEqualTo("rds.amazonaws.com");
        assertThat(dns.getPort()).isEqualTo(3306);
    }

    @Test
    void parseRdsDnsWithoutPort() {
        RdsDns dns = RdsDns.parse("us-east-1.rds.amazonaws.com");

        assertThat(dns.getRegion()).isEqualTo("us-east-1");
        assertThat(dns.getDomain()).isEqualTo("rds.amazonaws.com");
        assertThat(dns.getPort()).isEqualTo(null);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "us-east-1.rds.amazonaws.com:3306",
        "us-east-1.rds.amazonaws.com",
    })
    void shouldSerializerToOriginalValue(String input) {
        String output = RdsDns.parse(input).toString();

        assertThat(output).isEqualTo(input);
    }
}
