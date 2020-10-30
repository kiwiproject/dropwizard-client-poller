package org.kiwiproject.dropwizard.poller.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;
import org.kiwiproject.dropwizard.poller.metrics.DefaultClientPollerStatistics;

import java.net.URI;

@DisplayName("ClientPollerMissedPollHealthCheck")
class ClientPollerMissedPollHealthCheckTest {

    private static final long POLLER_INTERVAL_MILLIS = 5_000L;

    private ClientPoller poller;
    private DefaultClientPollerStatistics statistics;

    @BeforeEach
    void setUp() {
        statistics = new DefaultClientPollerStatistics();
        poller = mockPollerWith(statistics, POLLER_INTERVAL_MILLIS);
    }

    @Test
    void testNameFor_WithURI() {
        assertThat(ClientPollerMissedPollHealthCheck.nameFor(URI.create("http://localhost:8765/some/endpoint")))
                .isEqualTo("client-poller-missedPoll:http://localhost:8765/some/endpoint");
    }

    @Test
    void testNameFor_WithString() {
        assertThat(ClientPollerMissedPollHealthCheck.nameFor("http://localhost:8765/some/endpoint"))
                .isEqualTo("client-poller-missedPoll:http://localhost:8765/some/endpoint");
    }

    @Nested
    class FactoryMethods {

        @Test
        void shouldNotAllowANullPoller() {
            assertThatThrownBy(() -> ClientPollerMissedPollHealthCheck.of(null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldNotAllowANegativeMissingPollMultiplier() {
            assertThatThrownBy(() -> ClientPollerMissedPollHealthCheck.of(poller, -5))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void shouldUseDefaultMultiplierIfNotSupplied() {
            var healthCheck = ClientPollerMissedPollHealthCheck.of(poller);

            assertThat(healthCheck.getMissingPollMultiplier())
                    .isEqualTo(ClientPollerMissedPollHealthCheck.DEFAULT_MISSING_POLL_MULTIPLIER);

            assertThat(healthCheck.getMissingPollAlertThresholdMillis())
                    .isEqualTo(POLLER_INTERVAL_MILLIS * ClientPollerMissedPollHealthCheck.DEFAULT_MISSING_POLL_MULTIPLIER);
        }

        @Test
        void shouldUseProvidedMultiplierInsteadOfDefaultIfSupplied() {
            var multiplier = 15;

            var healthCheck = ClientPollerMissedPollHealthCheck.of(poller, multiplier);

            assertThat(healthCheck.getMissingPollMultiplier()).isEqualTo(multiplier);
            assertThat(healthCheck.getMissingPollAlertThresholdMillis()).isEqualTo(POLLER_INTERVAL_MILLIS * multiplier);
        }

        @Test
        void shouldPullMultiplierFromConfig_WhenProvided() {
            var multiplier = 15;

            var healthCheck = ClientPollerMissedPollHealthCheck.of(poller, PollerHealthCheckConfig.builder().missingPollMultiplier(multiplier).build());

            assertThat(healthCheck.getMissingPollMultiplier()).isEqualTo(multiplier);
            assertThat(healthCheck.getMissingPollAlertThresholdMillis()).isEqualTo(POLLER_INTERVAL_MILLIS * multiplier);
        }
    }

    @Nested
    class Check {

        @Nested
        class IsUnHealthy {

            @Test
            void whenThereIsNoLastAttemptOrSkippedPollTime() {
                var healthCheck = ClientPollerMissedPollHealthCheck.of(poller);

                var result = healthCheck.check();
                assertThat(result.isHealthy()).isFalse();
                assertThat(result.getMessage()).isEqualTo("There is no last poll attempt or skipped poll time");
            }

            @Test
            void whenNegativeTimeDifferenceSinceLastPollAttempt() {
                runAndAssertNegativeTimeDifference(ClientPollerMissedPollHealthCheck.LastAttemptType.POLL);
            }

            @Test
            void whenNegativeTimeDifferenceSinceLastSkippedPoll() {
                runAndAssertNegativeTimeDifference(ClientPollerMissedPollHealthCheck.LastAttemptType.SKIP);
            }

            private void runAndAssertNegativeTimeDifference(ClientPollerMissedPollHealthCheck.LastAttemptType lastAttemptType) {
                var healthCheck = ClientPollerMissedPollHealthCheck.of(poller);
                var lastAttempt = incrementCountAndGetLastAttemptMillis(lastAttemptType, statistics);

                var kiwiEnv = mock(KiwiEnvironment.class);
                healthCheck.kiwiEnvironment = kiwiEnv;

                when(kiwiEnv.currentTimeMillis()).thenReturn(lastAttempt - 1);

                var result = healthCheck.check();

                assertThat(result.isHealthy()).isFalse();
                assertThat(result.getMessage())
                        .startsWith("Negative time difference since last " + lastAttemptType.description)
                        .contains("current time millis is " + kiwiEnv.currentTimeMillis())
                        .contains("last attempt was " + lastAttempt)
                        .contains("delta millis is -1");
            }

            @Test
            void whenTimeSinceLastPoll_ExceedsAlertThreshold() {
                runAndAssertTimeSinceLastAttemptExceedsThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType.POLL);
            }

            @Test
            void whenTimeSinceLastSkip_ExceedsAlertThreshold() {
                runAndAssertTimeSinceLastAttemptExceedsThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType.SKIP);
            }

            private void runAndAssertTimeSinceLastAttemptExceedsThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType lastAttemptType) {
                var clientPoller = mockPollerWith(statistics, 10_000L);
                var healthCheck = ClientPollerMissedPollHealthCheck.of(clientPoller, 2);
                var lastAttempt = incrementCountAndGetLastAttemptMillis(lastAttemptType, statistics);

                var kiwiEnv = mock(KiwiEnvironment.class);
                healthCheck.kiwiEnvironment = kiwiEnv;

                when(kiwiEnv.currentTimeMillis()).thenReturn(lastAttempt + healthCheck.getMissingPollAlertThresholdMillis() + 1_000);

                var result = healthCheck.check();

                assertThat(result.isHealthy()).isFalse();
                assertThat(result.getMessage())
                        .startsWith("Time since last " + lastAttemptType.description)
                        .contains(" (21000 millis) ")
                        .endsWith(" exceeds alert threshold (20000 millis)");
            }
        }

        @Nested
        class IsHealthy {

            @Test
            void whenTimeSinceLastPoll_IsWithinTheAlertThreshold() {
                runAndAssertWhenTimeSinceLastAttemptIsWithinAlertThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType.POLL);
            }

            @Test
            void whenTimeSinceLastSkip_IsWithinTheAlertThreshold() {
                runAndAssertWhenTimeSinceLastAttemptIsWithinAlertThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType.SKIP);
            }

            private void runAndAssertWhenTimeSinceLastAttemptIsWithinAlertThreshold(ClientPollerMissedPollHealthCheck.LastAttemptType lastAttemptType) {
                var healthCheck = ClientPollerMissedPollHealthCheck.of(poller, 4);
                var lastAttempt = incrementCountAndGetLastAttemptMillis(lastAttemptType, statistics);

                var kiwiEnv = mock(KiwiEnvironment.class);
                healthCheck.kiwiEnvironment = kiwiEnv;

                long millisSinceLastPoll = POLLER_INTERVAL_MILLIS / 2;
                when(kiwiEnv.currentTimeMillis()).thenReturn(lastAttempt + millisSinceLastPoll);

                var result = healthCheck.check();

                assertThat(result.isHealthy()).isTrue();
                assertThat(result.getMessage())
                        .startsWith("Time since last " + lastAttemptType.description)
                        .endsWith(" is " + millisSinceLastPoll + " millis (threshold is 20000 millis)");
            }
        }

        private long incrementCountAndGetLastAttemptMillis(ClientPollerMissedPollHealthCheck.LastAttemptType lastAttemptType,
                                                           DefaultClientPollerStatistics statistics) {

            if (lastAttemptType == ClientPollerMissedPollHealthCheck.LastAttemptType.POLL) {
                statistics.incrementCount();
                return statistics.lastAttemptTimeInMillis().orElseThrow(IllegalStateException::new);
            }

            statistics.incrementSkipCount();
            return statistics.lastSkipTimeInMillis().orElseThrow(IllegalStateException::new);
        }
    }

    private static ClientPoller mockPollerWith(ClientPollerStatistics statistics, long executionIntervalMillis) {
        var thePoller = mock(ClientPoller.class);
        when(thePoller.statistics()).thenReturn(statistics);
        when(thePoller.getExecutionInterval()).thenReturn(executionIntervalMillis);
        return thePoller;
    }
}
