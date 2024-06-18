package org.kiwiproject.dropwizard.poller.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.base.KiwiStrings.format;
import static org.kiwiproject.metrics.health.HealthCheckResults.SEVERITY_DETAIL;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.dropwizard.util.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Stream;

@DisplayName("ClientPollerTimeBasedHealthCheck")
class ClientPollerTimeBasedHealthCheckTest {

    private static final long TIME_WINDOW_MILLIS = TimeUnit.MINUTES.toMillis(15);
    private static final int FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT = 2;
    private static final double UNHEALTHY_FAILURE_THRESHOLD = FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT / 100.0;

    private ClientPollerTimeBasedHealthCheck healthCheck;
    private ClientPollerStatistics statistics;
    private long now;
    private KiwiEnvironment kiwiEnv;

    @BeforeEach
    void setUp() {
        statistics = mock(ClientPollerStatistics.class);
        when(statistics.maxRecentFailureTimes()).thenReturn(100);

        kiwiEnv = mock(KiwiEnvironment.class);
        now = System.currentTimeMillis();
        when(kiwiEnv.currentTimeMillis()).thenReturn(now);

        healthCheck = ClientPollerTimeBasedHealthCheck.of(1_000, ChronoUnit.MILLIS, FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT,
                statistics, 15, ChronoUnit.MINUTES, kiwiEnv);
    }

    @Nested
    class NameFor {

        @Test
        void shouldBuildName_WhenGiven_URI() {
            var uri = URI.create("http://localhost:8765/some/endpoint");
            assertThat(ClientPollerTimeBasedHealthCheck.nameFor(uri)).isEqualTo("client-poller-time:" + uri);
        }

        @Test
        void shouldBuildName_WhenGiven_URIAsString() {
            var uri = "http://localhost:8765/some/endpoint";
            assertThat(ClientPollerTimeBasedHealthCheck.nameFor(uri)).isEqualTo("client-poller-time:" + uri);
        }

    }

    @Nested
    class FactoryMethods {

        @Test
        void shouldCreateHealthCheck_WithDefaultTimeWindow() {
            healthCheck = ClientPollerTimeBasedHealthCheck.of(5, ChronoUnit.SECONDS, statistics);
            assertThat(healthCheck.getTimeWindowMillis()).isEqualTo(TimeUnit.MINUTES.toMillis(15));
        }

        @Test
        void shouldCreateHealthCheck_WithSpecifiedTimeWindow() {
            healthCheck = ClientPollerTimeBasedHealthCheck.of(5, ChronoUnit.SECONDS, statistics, 1, ChronoUnit.HOURS);
            assertThat(healthCheck.getTimeWindowMillis()).isEqualTo(TimeUnit.HOURS.toMillis(1));
        }

        @Test
        void shouldCreateHealthCheck_WithSpecifiedConfiguration() {
            var timeWindow = Duration.minutes(10);
            var pollIntervalMillis = 1_500;

            var healthCheckConfig = PollerHealthCheckConfig.builder()
                    .timeWindow(timeWindow)
                    .averageLatencyWarningThreshold(Duration.seconds(5))
                    .failedPollsThresholdPercent(3)
                    .missingPollMultiplier(15)
                    .build();

            var poller = ClientPoller.builder()
                    .executionInterval(pollIntervalMillis)
                    .build();

            healthCheck = ClientPollerTimeBasedHealthCheck.of(poller, healthCheckConfig);

            assertThat(healthCheck.getPollIntervalMillis()).isEqualTo(pollIntervalMillis);
            assertThat(healthCheck.getTimeWindowMillis()).isEqualTo(timeWindow.toMilliseconds());
            assertThat(healthCheck.getFailedPollsUnhealthyThresholdPercent()).isEqualTo(3);
        }

    }

    @Nested
    class ShouldBeHealthy {

        @Test
        void whenNoFailures_InLastWindow() {
            healthCheck = ClientPollerTimeBasedHealthCheck.of(1_000, ChronoUnit.MILLIS, statistics);

            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.empty());

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage()).isEqualTo("No poller error(s) have occurred in last 15 minutes");
        }

        @Test
        void whenOnlyOneFailure_AtBeginningOfTimeWindow() {
            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beginningOfTimeWindow));
            when(statistics.recentFailureTimesInMillis()).thenReturn(Stream.of(beginningOfTimeWindow));

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage()).isEqualTo("1 poller error(s) have occurred in last 15 minutes (healthy b/c below calculated 2% threshold of 18 errors)");
        }

        @Test
        void whenFailureCount_IsEqualToThreshold() {
            var maxPollFailuresAllowed = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(TIME_WINDOW_MILLIS, 1_000,
                    0.0, UNHEALTHY_FAILURE_THRESHOLD);

            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beginningOfTimeWindow));

            var failureTimeStream = Stream.iterate(beginningOfTimeWindow, value -> value + 1_000).limit(maxPollFailuresAllowed);
            when(statistics.recentFailureTimesInMillis()).thenReturn(failureTimeStream);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage())
                    .isEqualTo(format("{} poller error(s) have occurred in last 15 minutes (healthy b/c below calculated 2% threshold of {} errors)",
                            maxPollFailuresAllowed, maxPollFailuresAllowed));
        }

        @Test
        void whenFailureCount_IsOneBelowThreshold() {
            var maxPollFailuresAllowed = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(TIME_WINDOW_MILLIS, 1_000,
                    0.0, UNHEALTHY_FAILURE_THRESHOLD);

            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beginningOfTimeWindow));

            var oneBelowThreshold = maxPollFailuresAllowed - 1;
            var failureTimeStream = Stream.iterate(beginningOfTimeWindow, value -> value + 1_000).limit(oneBelowThreshold);
            when(statistics.recentFailureTimesInMillis()).thenReturn(failureTimeStream);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage())
                    .isEqualTo(format("{} poller error(s) have occurred in last 15 minutes (healthy b/c below calculated 2% threshold of {} errors)",
                            oneBelowThreshold, maxPollFailuresAllowed));
        }

        @Test
        void whenOneFailure_BeforeTheBeginningOfTimeWindow() {
            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            var beforeBeginningOfTimeWindow = beginningOfTimeWindow - 1;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beforeBeginningOfTimeWindow));
            when(statistics.recentFailureTimesInMillis()).thenReturn(Stream.of(beforeBeginningOfTimeWindow));

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage()).isEqualTo("No poller error(s) have occurred in last 15 minutes");
        }
    }

    @Nested
    class ShouldBeUnhealthy {

        private int severityThresholdMultiplier;

        @BeforeEach
        void setUp() {
            severityThresholdMultiplier = 3;
            healthCheck = new ClientPollerTimeBasedHealthCheck(1_000, ChronoUnit.MILLIS,
                    FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT,
                    severityThresholdMultiplier,
                    statistics,
                    15, ChronoUnit.MINUTES,
                    kiwiEnv);
        }

        @Test
        void whenFailureCount_IsOneMoreThanThreshold() {
            var maxPollFailuresAllowed = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(TIME_WINDOW_MILLIS, 1_000,
                    0.0, UNHEALTHY_FAILURE_THRESHOLD);

            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beginningOfTimeWindow));

            var oneAboveThreshold = 1 + maxPollFailuresAllowed;
            var failureTimeStream = Stream.iterate(beginningOfTimeWindow, value -> value + 1_000).limit(oneAboveThreshold);
            when(statistics.recentFailureTimesInMillis()).thenReturn(failureTimeStream);

            var result = healthCheck.check();

            assertThat(result)
                    .isUnhealthy()
                    .hasMessage(format("{} poller error(s) have occurred in last 15 minutes (above calculated 2% threshold of {} errors, " +
                                    "using {} as severity multiplier)",
                            oneAboveThreshold, maxPollFailuresAllowed, severityThresholdMultiplier))
                    .hasDetail(SEVERITY_DETAIL, "WARN");
        }

        @Test
        void whenFailureCount_IsEqualToThresholdWithMultiplier() {
            assertUnhealthyWithComputedFailureCount("WARN",
                    (maxPollFailuresAllowed, theSeverityThresholdMultiplier) -> theSeverityThresholdMultiplier * maxPollFailuresAllowed);
        }

        @Test
        void whenFailureCount_IsOneMoreThanThresholdWithMultiplier() {
            assertUnhealthyWithComputedFailureCount("CRITICAL",
                    (maxPollFailuresAllowed, theSeverityThresholdMultiplier) -> 1 + (theSeverityThresholdMultiplier * maxPollFailuresAllowed));
        }

        private void assertUnhealthyWithComputedFailureCount(String expectedSeverity, BiFunction<Long, Integer, Long> failureCountFn) {
            var maxPollFailuresAllowed = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(TIME_WINDOW_MILLIS, 1_000,
                    0.0, UNHEALTHY_FAILURE_THRESHOLD);
            var numberOfFailures = failureCountFn.apply(maxPollFailuresAllowed, severityThresholdMultiplier);

            var beginningOfTimeWindow = now - TIME_WINDOW_MILLIS;
            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(beginningOfTimeWindow));

            var failureTimeStream = Stream.iterate(beginningOfTimeWindow, value -> value + 1_000).limit(numberOfFailures);
            when(statistics.recentFailureTimesInMillis()).thenReturn(failureTimeStream);

            var result = healthCheck.check();

            assertThat(result)
                    .isUnhealthy()
                    .hasMessage(format("{} poller error(s) have occurred in last 15 minutes (above calculated 2% threshold of {} errors, " +
                                    "using {} as severity multiplier)",
                            numberOfFailures, maxPollFailuresAllowed, severityThresholdMultiplier))
                    .hasDetail(SEVERITY_DETAIL, expectedSeverity);
        }

        @Test
        void whenFailureCount_IsMoreThanMaximumNumberOfRecentFailures() {
            var maxPollFailuresAllowed = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(TIME_WINDOW_MILLIS, 1_000,
                    0.0, UNHEALTHY_FAILURE_THRESHOLD);
            var numberOfFailures = statistics.maxRecentFailureTimes() + 1;

            when(statistics.lastFailureTimeInMillis()).thenReturn(Optional.of(now));

            var recentFailureTimes = Stream.iterate(now, value -> value - 1_000).limit(numberOfFailures);
            when(statistics.recentFailureTimesInMillis()).thenReturn(recentFailureTimes);

            var result = healthCheck.check();

            assertThat(result)
                    .isUnhealthy()
                    .hasMessage(format("At least {} (the max number of retained recent failures) poller error(s) have occurred in last 15 minutes " +
                                    "(above calculated 2% threshold of {} errors, using {} as severity multiplier)",
                            statistics.maxRecentFailureTimes(), maxPollFailuresAllowed, severityThresholdMultiplier))
                    .hasDetail(SEVERITY_DETAIL, "CRITICAL");
        }
    }

    @Nested
    class CalculateMaxPollFailuresAllowed {

        private long timeWindow;
        private long pollInterval;

        @BeforeEach
        void setUp() {
            timeWindow = 1_000_000;
            pollInterval = 1_000;
        }

        @ParameterizedTest
        @CsvSource({
                "0.0, 20",
                "50.0, 19",
                "100.0, 18",
                "500.0, 13",
                "900.0, 11",
                "1000.0, 10",
                "1800.0, 7",
                "2000.0, 7",
                "9000.0, 2",
                "10000.0, 2"
        })
        void shouldRoundMaxAllowedFailures(double avgPollLatency, long expectedMaxAllowed) {
            var max = ClientPollerTimeBasedHealthCheck.calculateMaxPollFailuresAllowed(timeWindow, pollInterval, avgPollLatency,
                    UNHEALTHY_FAILURE_THRESHOLD);

            assertThat(max).isEqualTo(expectedMaxAllowed);
        }
    }
}
