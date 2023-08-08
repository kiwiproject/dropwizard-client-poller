package org.kiwiproject.dropwizard.poller.config;

import static java.util.Objects.isNull;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck.DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck.DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck.DEFAULT_TIME_WINDOW_MINUTES;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Duration;
import io.dropwizard.validation.MaxDuration;
import io.dropwizard.validation.MinDuration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.Builder;
import lombok.Value;
import org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck;
import org.kiwiproject.dropwizard.poller.health.ClientPollerMissedPollHealthCheck;
import org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck;

import java.util.concurrent.TimeUnit;

/**
 * Specifies configuration properties for the various poller health checks. Some configuration properties apply
 * to only one specific health check.
 */
@Value
public class PollerHealthCheckConfig {

    /**
     * The time window the health checks should consider when checking health. Required.
     * <p>
     * Default: {@link ClientPollerTimeBasedHealthCheck#DEFAULT_TIME_WINDOW_MINUTES} minutes.
     *
     * @see ClientPollerTimeBasedHealthCheck
     */
    @MinDuration(value = 1, unit = TimeUnit.MINUTES)
    @MaxDuration(value = 24, unit = TimeUnit.HOURS)
    Duration timeWindow;

    /**
     * The percent (as a whole number) of attempted pools above which the time-based health check will report as
     * unhealthy. Required.
     * <p>
     * Default: {@link ClientPollerTimeBasedHealthCheck#DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT}
     *
     * @see ClientPollerTimeBasedHealthCheck
     */
    @Min(1)
    @Max(99)
    Integer failedPollsThresholdPercent;

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
     * For example, at a value of 10, and assuming a 2-second interval between polls, then polling will be
     * reported unhealthy if the last poll attempt is more than 20 seconds ago (10 * 2).
     *
     * @see ClientPollerMissedPollHealthCheck
     */
    @Min(5)
    @Max(50)
    Integer missingPollMultiplier;

    @JsonCreator
    @Builder
    private PollerHealthCheckConfig(@JsonProperty("timeWindow") Duration timeWindow,
                                    @JsonProperty("failedPollsThresholdPercent") Integer failedPollsThresholdPercent,
                                    @JsonProperty("averageLatencyWarningThreshold") Duration averageLatencyWarningThreshold,
                                    @JsonProperty("missingPollMultiplier") Integer missingPollMultiplier) {

        this.timeWindow = isNull(timeWindow) ? Duration.minutes(DEFAULT_TIME_WINDOW_MINUTES) : timeWindow;

        this.failedPollsThresholdPercent = isNull(failedPollsThresholdPercent) ? DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT : failedPollsThresholdPercent;

        this.averageLatencyWarningThreshold = isNull(averageLatencyWarningThreshold) ?
                Duration.milliseconds(DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS) : averageLatencyWarningThreshold;

        this.missingPollMultiplier = isNull(missingPollMultiplier) ?
                ClientPollerMissedPollHealthCheck.DEFAULT_MISSING_POLL_MULTIPLIER : missingPollMultiplier;
    }
}
