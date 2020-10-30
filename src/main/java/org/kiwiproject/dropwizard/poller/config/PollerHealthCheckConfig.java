package org.kiwiproject.dropwizard.poller.config;

import static java.util.Objects.isNull;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck.DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MaxDuration;
import io.dropwizard.validation.MinDuration;
import lombok.Builder;
import lombok.Value;
import org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck;

import java.util.concurrent.TimeUnit;

/**
 * Specifies configuration properties for the various poller health checks. Some configuration properties apply
 * to only one specific health check.
 */
@Value
public class PollerHealthCheckConfig {

    /**
     * The average latency for polls above which the latency-based health check will report as unhealthy. Required.
     * <p>
     * Default: {@link ClientPollerLatencyBasedHealthCheck#DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS} milliseconds
     *
     * @see ClientPollerLatencyBasedHealthCheck
     */
    @MinDuration(value = 5, unit = TimeUnit.MILLISECONDS)
    @MaxDuration(value = 60, unit = TimeUnit.MINUTES)
    Duration averageLatencyWarningThreshold;

    @JsonCreator
    @Builder
    private PollerHealthCheckConfig(@JsonProperty("averageLatencyWarningThreshold") Duration averageLatencyWarningThreshold) {
        this.averageLatencyWarningThreshold = isNull(averageLatencyWarningThreshold) ?
                Duration.milliseconds(DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS) : averageLatencyWarningThreshold;
    }
}
