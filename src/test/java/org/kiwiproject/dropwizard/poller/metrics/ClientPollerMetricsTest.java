package org.kiwiproject.dropwizard.poller.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.core.setup.Environment;
import jakarta.ws.rs.client.SyncInvoker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.poller.ClientPoller;

import java.util.concurrent.Executors;

@DisplayName("ClientPollerMetrics")
class ClientPollerMetricsTest {

    private ClientPoller poller;
    private MetricRegistry registry;

    @BeforeEach
    void setUp() {
        poller = ClientPoller.builder()
                .name("TestPoller")
                .supplier(() -> mock(SyncInvoker.class))
                .consumer(response -> {})
                .executor(Executors.newSingleThreadScheduledExecutor())
                .executionInterval(1000)
                .build();

        registry = new MetricRegistry();
    }

    @Nested
    class MetricName {

        @Test
        void shouldBuildNameFromPollerNameAndSuffix() {
            assertThat(ClientPollerMetrics.metricName("MyPoller", "count"))
                    .isEqualTo("ClientPoller.MyPoller.count");
        }

        @Test
        void shouldBuildNameWithHyphenatedSuffix() {
            assertThat(ClientPollerMetrics.metricName("MyPoller", "success-count"))
                    .isEqualTo("ClientPoller.MyPoller.success-count");
        }
    }

    @Nested
    class RegisterPollerMetrics {

        @Test
        void shouldRegisterAllGaugesWithMetricRegistry() {
            var registeredNames = ClientPollerMetrics.registerPollerMetrics(poller, registry);

            assertThat(registeredNames).containsExactlyInAnyOrder(
                    "ClientPoller.TestPoller.count",
                    "ClientPoller.TestPoller.success-count",
                    "ClientPoller.TestPoller.failure-count",
                    "ClientPoller.TestPoller.skip-count",
                    "ClientPoller.TestPoller.average-poll-latency-ms"
            );

            assertThat(registry.getGauges()).containsOnlyKeys(
                    "ClientPoller.TestPoller.count",
                    "ClientPoller.TestPoller.success-count",
                    "ClientPoller.TestPoller.failure-count",
                    "ClientPoller.TestPoller.skip-count",
                    "ClientPoller.TestPoller.average-poll-latency-ms"
            );
        }

        @Test
        void shouldRegisterAllGaugesWithEnvironment() {
            var env = mock(Environment.class);
            when(env.metrics()).thenReturn(registry);

            ClientPollerMetrics.registerPollerMetrics(poller, env);

            assertThat(registry.getGauges()).containsOnlyKeys(
                    "ClientPoller.TestPoller.count",
                    "ClientPoller.TestPoller.success-count",
                    "ClientPoller.TestPoller.failure-count",
                    "ClientPoller.TestPoller.skip-count",
                    "ClientPoller.TestPoller.average-poll-latency-ms"
            );
        }

        @Test
        void shouldReflectCurrentStatisticsValues() {
            ClientPollerMetrics.registerPollerMetrics(poller, registry);

            var stats = poller.statistics();
            stats.incrementCount();
            stats.incrementSuccessCount();
            stats.incrementSuccessCount();
            stats.incrementFailureCount("error");
            stats.incrementSkipCount();
            stats.addPollLatencyMeasurement(100);

            var gauges = registry.getGauges();

            assertThat((Gauge<Integer>) gauges.get("ClientPoller.TestPoller.count")).extracting(Gauge::getValue).isEqualTo(1);
            assertThat((Gauge<Integer>) gauges.get("ClientPoller.TestPoller.success-count")).extracting(Gauge::getValue).isEqualTo(2);
            assertThat((Gauge<Integer>) gauges.get("ClientPoller.TestPoller.failure-count")).extracting(Gauge::getValue).isEqualTo(1);
            assertThat((Gauge<Integer>) gauges.get("ClientPoller.TestPoller.skip-count")).extracting(Gauge::getValue).isEqualTo(1);
            assertThat((Gauge<Double>) gauges.get("ClientPoller.TestPoller.average-poll-latency-ms")).extracting(Gauge::getValue).isEqualTo(100.0);
        }
    }
}
