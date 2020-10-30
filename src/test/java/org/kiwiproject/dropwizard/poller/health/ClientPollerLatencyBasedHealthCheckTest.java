package org.kiwiproject.dropwizard.poller.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;

import java.net.URI;
import java.time.temporal.ChronoUnit;

@DisplayName("ClientPollerLatencyBasedHealthCheck")
class ClientPollerLatencyBasedHealthCheckTest {

    private ClientPollerLatencyBasedHealthCheck healthCheck;
    private ClientPollerStatistics statistics;

    @BeforeEach
    void setUp() {
        statistics = mock(ClientPollerStatistics.class);
        healthCheck = ClientPollerLatencyBasedHealthCheck.of(statistics, 1, ChronoUnit.SECONDS);
    }

    @Nested
    class NameFor {

        @Test
        void shouldBuildName_WhenGiven_URI() {
            var uri = URI.create("http://localhost:8765/some/endpoint");
            assertThat(ClientPollerLatencyBasedHealthCheck.nameFor(uri)).isEqualTo("client-poller-latency:" + uri);
        }

        @Test
        void shouldBuildName_WhenGiven_URIAsString() {
            var uri = "http://localhost:8765/some/endpoint";
            assertThat(ClientPollerLatencyBasedHealthCheck.nameFor(uri)).isEqualTo("client-poller-latency:" + uri);
        }

    }

    @Nested
    class FactoryMethods {

        @Test
        void shouldCreateHealthCheck_WithDefaultAverageLatency() {
            healthCheck = ClientPollerLatencyBasedHealthCheck.of(statistics);

            assertThat(healthCheck.getAvgLatencyWarningThresholdMillis())
                    .isEqualTo(ClientPollerLatencyBasedHealthCheck.DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS);
        }

        @Test
        void shouldCreateHealthCheck_FromClientPoller() {
            var poller = mock(ClientPoller.class);
            when(poller.statistics()).thenReturn(statistics);

            healthCheck = ClientPollerLatencyBasedHealthCheck.of(poller, PollerHealthCheckConfig.builder().build());
            assertThat(healthCheck.getAvgLatencyWarningThresholdMillis())
                    .isEqualTo(ClientPollerLatencyBasedHealthCheck.DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS);
        }

    }

    @Nested
    class ShouldBeHealthy {

        @Test
        void whenAverageLatency_IsBelowWarningThreshold() {
            when(statistics.averagePollLatencyInMillis()).thenReturn(760.62578);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage()).isEqualTo("Poller average latency: 760.63 millis");
        }

        @Test
        void whenAverageLatency_IsBarelyBelowWarningThreshold() {
            when(statistics.averagePollLatencyInMillis()).thenReturn(999.99999999);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isTrue();
            assertThat(result.getMessage()).isEqualTo("Poller average latency: 1000.00 millis");
        }

    }

    @Nested
    class ShouldBeUnhealthy {

        @Test
        void whenAverageLatency_IsBarelyAboveWarningThreshold() {
            when(statistics.averagePollLatencyInMillis()).thenReturn(1000.000001);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isFalse();
            assertThat(result.getMessage()).isEqualTo("Poller average latency 1000.00 millis is barely above warning threshold (1000.00)");
        }

        @Test
        void whenAverageLatency_IsAboveWarningThreshold() {
            when(statistics.averagePollLatencyInMillis()).thenReturn(1250.6789);

            var result = healthCheck.check();
            assertThat(result.isHealthy()).isFalse();
            assertThat(result.getMessage()).isEqualTo("Poller average latency 1250.68 millis is above warning threshold (1000.00)");
        }
    }
}
