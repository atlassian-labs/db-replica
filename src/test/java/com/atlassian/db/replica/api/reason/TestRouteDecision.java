package com.atlassian.db.replica.api.reason;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class TestRouteDecision {

    private static final RouteDecision READ_DECISION = new RouteDecision("select * from somewhere", Reason.READ_OPERATION, null);
    private static final RouteDecision WRITE_DECISION = new RouteDecision("select * from somewhere", Reason.WRITE_OPERATION, null);
    private static final RouteDecision WRITE_DECISION2 = new RouteDecision("select * from somewhere", Reason.LOCK, null);
    private static final RouteDecision WRITE_DECISION3 = new RouteDecision("select * from somewhere", Reason.RW_API_CALL, null);

    @Parameters
    public static Collection routeDecisions() {
        return Arrays.asList(new Object[][]{
                {READ_DECISION, false},
                {WRITE_DECISION, true},
                {WRITE_DECISION2, true},
                {WRITE_DECISION3, true},
        });
    }

    private RouteDecision givenRouteDecision;
    private boolean expectedIsWrite;

    public TestRouteDecision(RouteDecision givenRouteDecision, boolean expectedIsWrite) {
        this.givenRouteDecision = givenRouteDecision;
        this.expectedIsWrite = expectedIsWrite;
    }

    @Test
    public void shouldDetectWriteDatabaseCallAndRunReleatedObservedAction() {
        assertThat(givenRouteDecision.isWrite()).isEqualTo(expectedIsWrite);
    }

}