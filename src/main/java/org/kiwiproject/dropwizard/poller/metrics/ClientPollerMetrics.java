package org.kiwiproject.dropwizard.poller.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.core.setup.Environment;
import lombok.experimental.UtilityClass;
import org.kiwiproject.dropwizard.poller.ClientPoller;

import java.util.List;

/**
 * Utility class for registering {@link ClientPoller} statistics as Dropwizard Metrics gauges,
 * making them available via the standard metrics endpoint and any configured metrics reporter.
 */
@UtilityClass
public class ClientPollerMetrics {

    /**
     * Registers gauges for all standard {@link ClientPollerStatistics} fields into the given Dropwizard environment's
     * {@link MetricRegistry}.
     *
     * @param poller      the poller whose statistics to expose
     * @param environment the Dropwizard environment
     * @return the list of registered metric names
     */
    public static List<String> registerPollerMetrics(ClientPoller poller, Environment environment) {
        return registerPollerMetrics(poller, environment.metrics());
    }

    /**
     * Registers gauges for all standard {@link ClientPollerStatistics} fields into the given {@link MetricRegistry}.
     *
     * @param poller   the poller whose statistics to expose
     * @param registry the metric registry
     * @return the list of registered metric names
     * @throws IllegalArgumentException if any of the metric names are already registered
     */
    public static List<String> registerPollerMetrics(ClientPoller poller, MetricRegistry registry) {
        var stats = poller.statistics();
        var name = poller.getName();

        var countName = metricName(name, "count");
        var successCountName = metricName(name, "success-count");
        var failureCountName = metricName(name, "failure-count");
        var skipCountName = metricName(name, "skip-count");
        var avgLatencyName = metricName(name, "average-poll-latency-ms");

        registry.register(countName, (Gauge<Integer>) stats::totalCount);
        registry.register(successCountName, (Gauge<Integer>) stats::successCount);
        registry.register(failureCountName, (Gauge<Integer>) stats::failureCount);
        registry.register(skipCountName, (Gauge<Integer>) stats::skipCount);
        registry.register(avgLatencyName, (Gauge<Double>) stats::averagePollLatencyInMillis);

        return List.of(countName, successCountName, failureCountName, skipCountName, avgLatencyName);
    }

    /**
     * Returns the metric name for the given poller name and metric suffix, using the standard
     * {@code ClientPoller.<pollerName>.<suffix>} naming convention.
     *
     * @param pollerName   the name of the poller
     * @param metricSuffix the metric suffix (e.g. {@code "count"}, {@code "success-count"})
     * @return the full metric name
     */
    public static String metricName(String pollerName, String metricSuffix) {
        return MetricRegistry.name("ClientPoller", pollerName, metricSuffix);
    }
}
