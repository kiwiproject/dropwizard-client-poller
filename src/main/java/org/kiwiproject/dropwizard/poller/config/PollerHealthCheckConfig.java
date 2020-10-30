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
import org.kiwiproject.dropwizard.poller.health.ClientPollerMissedPollHealthCheck;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
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

    /**
     * The multiplier for computing the allowable time since the last attempted poll. Above the computed allowable time,
     * the missing-poll health check will report as unhealthy. Required.
     * <p>
     * Default: {@link ClientPollerMissedPollHealthCheck#DEFAULT_MISSING_POLL_MULTIPLIER}
     * <p>
     * For example, at a value of 10, and assuming a 2 second interval between polls, then polling will be
     * reported unhealthy if the last poll attempt is more than 20 seconds ago (10 * 2).
     *
     * @see ClientPollerMissedPollHealthCheck
     */
    @Min(5)
    @Max(50)
    Integer missingPollMultiplier;

    @JsonCreator
    @Builder
    private PollerHealthCheckConfig(@JsonProperty("averageLatencyWarningThreshold") Duration averageLatencyWarningThreshold,
                                    @JsonProperty("missingPollMultiplier") Integer missingPollMultiplier) {

        this.averageLatencyWarningThreshold = isNull(averageLatencyWarningThreshold) ?
                Duration.milliseconds(DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS) : averageLatencyWarningThreshold;

        this.missingPollMultiplier = isNull(missingPollMultiplier) ?
                ClientPollerMissedPollHealthCheck.DEFAULT_MISSING_POLL_MULTIPLIER : missingPollMultiplier;
    }
}
