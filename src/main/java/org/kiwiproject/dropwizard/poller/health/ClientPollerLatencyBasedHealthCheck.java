package org.kiwiproject.dropwizard.poller.health;

import static org.kiwiproject.dropwizard.poller.health.ClientPollerHealthChecks.humanReadableOf;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

@Slf4j
public class ClientPollerLatencyBasedHealthCheck extends HealthCheck {

    private static final String TYPE = "latency";

    /**
     * Default threshold amount for average latency above which this health check will report unhealthy.
     */
    public static final long DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS = 3_500;

    /**
     * Default threshold unit for average latency above which this health check will report unhealthy.
     */
    @VisibleForTesting
    static final TemporalUnit DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_UNIT = ChronoUnit.MILLIS;

    private final ClientPollerStatistics statistics;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    final double avgLatencyWarningThresholdMillis;

    private ClientPollerLatencyBasedHealthCheck(ClientPollerStatistics statistics,
                                                long avgLatencyWarningThreshold,
                                                TemporalUnit avgLatencyWarningThresholdUnit) {

        this.statistics = statistics;
        this.avgLatencyWarningThresholdMillis = avgLatencyWarningThresholdUnit.getDuration().toMillis() * (double) avgLatencyWarningThreshold;

        LOG.debug("Client poller latency-based health check using latency threshold {} {} ({} ms)",
                avgLatencyWarningThreshold,
                humanReadableOf(avgLatencyWarningThresholdUnit),
                avgLatencyWarningThresholdMillis);
    }

    /**
     * Return a name that can be used for a health check polling the given URI. Returns a generic name if the argument is null.
     *
     * @param pollingUri The uri of that is being polled
     * @return A name for the health check
     */
    public static String nameFor(URI pollingUri) {
        return ClientPollerHealthChecks.nameFor(TYPE, pollingUri);
    }

    public static String nameFor(String pollingUri) {
        return ClientPollerHealthChecks.nameFor(TYPE, pollingUri);
    }

    /**
     * Create instance using given {@link ClientPoller} and {@link PollerHealthCheckConfig}
     *
     * @param poller            The poller to pull the statistics from
     * @param healthCheckConfig The configuration for the health checks
     * @return a new instance of the health check
     */
    public static ClientPollerLatencyBasedHealthCheck of(ClientPoller poller, PollerHealthCheckConfig healthCheckConfig) {
        return of(
                poller.statistics(),
                healthCheckConfig.getAverageLatencyWarningThreshold().toMilliseconds(),
                ChronoUnit.MILLIS
        );
    }

    /**
     * Create instance using given statistics and a default latency threshold of {@link #DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS}.
     *
     * @param statistics The statistics to use for tracking latency health
     * @return a new instance of the health check
     */
    public static ClientPollerLatencyBasedHealthCheck of(ClientPollerStatistics statistics) {
        return of(statistics, DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS, DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_UNIT);
    }

    /**
     * Create instance using given statistics, and specified latency warning threshold.
     *
     * @param statistics                     The statistics to use for tracking latency health
     * @param avgLatencyWarningThreshold     The warning threshold for latency
     * @param avgLatencyWarningThresholdUnit The units that correspond to the avgLatencyWarningThresold
     * @return a new instance of the health check
     */
    public static ClientPollerLatencyBasedHealthCheck of(ClientPollerStatistics statistics,
                                                         long avgLatencyWarningThreshold,
                                                         TemporalUnit avgLatencyWarningThresholdUnit) {
        return new ClientPollerLatencyBasedHealthCheck(statistics, avgLatencyWarningThreshold, avgLatencyWarningThresholdUnit);
    }

    @Override
    protected Result check() {
        var avgPollLatencyInMillis = statistics.averagePollLatencyInMillis();

        if (avgPollLatencyInMillis < avgLatencyWarningThresholdMillis) {
            return newHealthyResult("Poller average latency: %.2f millis", avgPollLatencyInMillis);
        }

        return ClientPollerHealthChecks.unhealthy(
                statistics,
                "Poller average latency %.2f millis is %s warning threshold (%.2f)",
                avgPollLatencyInMillis,
                howMuchAbove(avgPollLatencyInMillis),
                avgLatencyWarningThresholdMillis
        );
    }

    private String howMuchAbove(double avgPollLatencyInMillis) {
        if (avgPollLatencyInMillis - avgLatencyWarningThresholdMillis < 1E-2) {
            return "barely above";
        }

        return "above";
    }
}
