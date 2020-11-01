package org.kiwiproject.dropwizard.poller.health;

import static org.kiwiproject.base.KiwiStrings.format;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerHealthChecks.humanReadableOf;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;
import org.kiwiproject.metrics.health.HealthStatus;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

@Slf4j
public class ClientPollerTimeBasedHealthCheck extends HealthCheck {

    /**
     * Defines the default threshold for failed polls within time window as 2%
     */
    public static final int DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT = 2;

    /**
     * Default time window amount in minutes
     */
    public static final long DEFAULT_TIME_WINDOW_MINUTES = 15;

    /**
     * If the number of failed polls exceeds the calculated threshold, we will report unhealthy at WARN severity
     * until the number of failures is higher than this multiplier times the allowed maximum number. Above that, we
     * will report unhealthy at CRITICAL severity.
     */
    private static final int DEFAULT_SEVERITY_THRESHOLD_MULTIPLIER = 11;

    private static final String TYPE = "time";
    private static final TemporalUnit DEFAULT_TIME_WINDOW_UNIT = ChronoUnit.MINUTES;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final long pollIntervalMillis;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final long timeWindowMillis;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final int failedPollsUnhealthyThresholdPercent;

    private final int severityThresholdMultiplier;
    private final ClientPollerStatistics statistics;
    private final double failedPollsUnhealthyThreshold;
    private final String pollerErrorsMessageFragment;
    private final String noPollFailuresMessage;
    private final KiwiEnvironment kiwiEnv;

    private ClientPollerTimeBasedHealthCheck(long pollInterval,
                                             TemporalUnit pollIntervalUnit,
                                             int failedPollsUnhealthyThresholdPercent,
                                             ClientPollerStatistics statistics,
                                             long timeWindowAmount,
                                             TemporalUnit timeWindowUnit,
                                             KiwiEnvironment kiwiEnv) {

        this(pollInterval,
                pollIntervalUnit,
                failedPollsUnhealthyThresholdPercent,
                DEFAULT_SEVERITY_THRESHOLD_MULTIPLIER,
                statistics,
                timeWindowAmount,
                timeWindowUnit,
                kiwiEnv);
    }

    @SuppressWarnings("java:S107")
    public ClientPollerTimeBasedHealthCheck(long pollInterval,
                                            TemporalUnit pollIntervalUnit,
                                            int failedPollsUnhealthyThresholdPercent,
                                            int severityThresholdMultiplier,
                                            ClientPollerStatistics statistics,
                                            long timeWindowAmount,
                                            TemporalUnit timeWindowUnit,
                                            KiwiEnvironment kiwiEnv) {

        this.pollIntervalMillis = pollIntervalUnit.getDuration().toMillis() * pollInterval;
        this.statistics = statistics;
        this.failedPollsUnhealthyThresholdPercent = failedPollsUnhealthyThresholdPercent;
        this.failedPollsUnhealthyThreshold = failedPollsUnhealthyThresholdPercent / 100.0;
        this.severityThresholdMultiplier = severityThresholdMultiplier;
        this.kiwiEnv = kiwiEnv;
        this.timeWindowMillis = timeWindowUnit.getDuration().toMillis() * timeWindowAmount;

        var humanizedUnits = humanReadableOf(timeWindowUnit);

        this.pollerErrorsMessageFragment = format(" poller error(s) have occurred in last {} {}", timeWindowAmount, humanizedUnits);
        this.noPollFailuresMessage = "No" + pollerErrorsMessageFragment;

        LOG.debug("Client poller time-based health check using poller interval {} {} ; time window {} {}; failed polls unhealthy threshold {}%",
                pollInterval, pollIntervalUnit, timeWindowAmount, humanizedUnits, failedPollsUnhealthyThresholdPercent);
    }

    /**
     * Return a name that can be used for a health check polling the given URI. Returns a generic name if the argument is null.
     *
     * @param pollingUri The URI to use in the name
     * @return a name for this health check
     */
    public static String nameFor(URI pollingUri) {
        return ClientPollerHealthChecks.nameFor(TYPE, pollingUri);
    }

    /**
     * Return a name that can be used for a health check polling the given URI as a string. Returns a generic name if the argument is null.
     *
     * @param pollingUri The URI as a string to use in the name
     * @return a name for this health check
     */
    public static String nameFor(String pollingUri) {
        return ClientPollerHealthChecks.nameFor(TYPE, pollingUri);
    }

    /**
     * Create a new {@link ClientPollerTimeBasedHealthCheck} for the given poller with the given
     * {@link PollerHealthCheckConfig}
     *
     * @param poller            The poller that the health check is monitoring
     * @param healthCheckConfig The config for the health checks
     * @return a new instance of this health check
     */
    public static ClientPollerTimeBasedHealthCheck of(ClientPoller poller, PollerHealthCheckConfig healthCheckConfig) {
        return of(
                poller.getExecutionInterval(),
                ChronoUnit.MILLIS,
                healthCheckConfig.getFailedPollsThresholdPercent(),
                poller.statistics(),
                healthCheckConfig.getTimeWindow().toMilliseconds(),
                ChronoUnit.MILLIS,
                new DefaultEnvironment());
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poll interval/units and with the given
     * {@link ClientPollerStatistics}
     *
     * @param pollInterval     The poll interval that the health check is monitoring
     * @param pollIntervalUnit The units for the poll interval
     * @param statistics       The statistics being used for monitoring
     * @return a new instance of this health check
     */
    public static ClientPollerTimeBasedHealthCheck of(long pollInterval,
                                                      TemporalUnit pollIntervalUnit,
                                                      ClientPollerStatistics statistics) {
        return of(
                pollInterval,
                pollIntervalUnit,
                statistics,
                DEFAULT_TIME_WINDOW_MINUTES,
                DEFAULT_TIME_WINDOW_UNIT
        );
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poll interval/units, with the given
     * {@link ClientPollerStatistics}, and with the given time window/units.
     *
     * @param pollInterval     The poll interval that the health check is monitoring
     * @param pollIntervalUnit The units for the poll interval
     * @param statistics       The statistics being used for monitoring
     * @param timeWindowAmount The time window to monitor
     * @param timeWindowUnit   The units for the time window
     * @return a new instance of this health check
     */
    public static ClientPollerTimeBasedHealthCheck of(long pollInterval,
                                                      TemporalUnit pollIntervalUnit,
                                                      ClientPollerStatistics statistics,
                                                      long timeWindowAmount,
                                                      TemporalUnit timeWindowUnit) {

        return of(
                pollInterval,
                pollIntervalUnit,
                DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT,
                statistics,
                timeWindowAmount,
                timeWindowUnit,
                new DefaultEnvironment()
        );
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poll interval/units, with the given
     * {@link ClientPollerStatistics}, with the given time window/units, and the given {@link KiwiEnvironment}.
     *
     * @param pollInterval     The poll interval that the health check is monitoring
     * @param pollIntervalUnit The units for the poll interval
     * @param statistics       The statistics being used for monitoring
     * @param timeWindowAmount The time window to monitor
     * @param timeWindowUnit   The units for the time window
     * @param kiwiEnv          The kiwi environment
     * @return a new instance of this health check
     */
    public static ClientPollerTimeBasedHealthCheck of(long pollInterval,
                                                      TemporalUnit pollIntervalUnit,
                                                      int failedPollsUnhealthyThresholdPercent,
                                                      ClientPollerStatistics statistics,
                                                      long timeWindowAmount,
                                                      TemporalUnit timeWindowUnit,
                                                      KiwiEnvironment kiwiEnv) {

        return new ClientPollerTimeBasedHealthCheck(pollInterval, pollIntervalUnit, failedPollsUnhealthyThresholdPercent, statistics,
                timeWindowAmount, timeWindowUnit, kiwiEnv);
    }

    @Override
    protected Result check() {
        long checkTime = kiwiEnv.currentTimeMillis();
        if (anyFailuresWithinTimeWindow(checkTime)) {
            return checkHealthWhenAtLeastOnePollError(checkTime);
        }

        return newHealthyResult(noPollFailuresMessage);
    }

    private boolean anyFailuresWithinTimeWindow(long checkTime) {
        return timeSinceLastFailure(checkTime) <= timeWindowMillis;
    }

    private long timeSinceLastFailure(long checkTime) {
        var lastFailure = statistics.lastFailureTimeInMillis().orElse(0L);
        return checkTime - lastFailure;
    }

    private Result checkHealthWhenAtLeastOnePollError(long checkTime) {
        var recentFailureCount = countNumberOfRecentFailures(checkTime);
        var maxPollFailuresAllowed = calculateMaxPollFailuresAllowed(timeWindowMillis, pollIntervalMillis, statistics.averagePollLatencyInMillis(),
                failedPollsUnhealthyThreshold);

        if (recentFailureCount <= maxPollFailuresAllowed) {
            var message = format("{}{} (healthy b/c below calculated {}% threshold of {} errors)",
                    recentFailureCount, pollerErrorsMessageFragment, failedPollsUnhealthyThresholdPercent, maxPollFailuresAllowed);

            return newHealthyResult(message);
        }

        var severity = determineSeverity(recentFailureCount, maxPollFailuresAllowed, severityThresholdMultiplier);

        // Must escape the % since this goes through Metrics library code which uses String.format
        return ClientPollerHealthChecks.unhealthy(
                statistics,
                severity,
                "%s%s (above calculated %d%% threshold of %d errors, using %d as severity multiplier)",
                unhealthyMessagePrefix(recentFailureCount, statistics.maxRecentFailureTimes()),
                pollerErrorsMessageFragment,
                failedPollsUnhealthyThresholdPercent,
                maxPollFailuresAllowed,
                severityThresholdMultiplier
        );
    }

    private static HealthStatus determineSeverity(long recentFailureCount,
                                                  long maxPollFailuresAllowed,
                                                  int severityThresholdMultiplier) {

        var maxFailuresAllowedWithMultiplier = severityThresholdMultiplier * maxPollFailuresAllowed;

        if (recentFailureCount <= maxFailuresAllowedWithMultiplier) {
            return HealthStatus.WARN;
        }

        return HealthStatus.CRITICAL;
    }

    private long countNumberOfRecentFailures(long checkTime) {
        var timeWindowLowerBound = checkTime - timeWindowMillis;
        return statistics.recentFailureTimesInMillis().filter(time -> time >= timeWindowLowerBound).count();
    }

    /**
     * @implNote The first three arguments MUST be in the same units, e.g. all milliseconds. The threshold argument
     * MUST be a decimal value, e.g. 0.03 (for 3%)
     */
    @VisibleForTesting
    static long calculateMaxPollFailuresAllowed(long timeWindow,
                                                long pollInterval,
                                                double avgPollLatency,
                                                double failedPollsUnhealthyThreshold) {
        double estimatedPollsInTimeWindow = timeWindow / (pollInterval + avgPollLatency);
        return Math.round(failedPollsUnhealthyThreshold * estimatedPollsInTimeWindow);
    }

    private static String unhealthyMessagePrefix(long numInQueue, int maxQueueSize) {
        if (numInQueue < maxQueueSize) {
            return String.valueOf(numInQueue);
        }

        return "At least " + maxQueueSize + " (the max number of retained recent failures)";
    }
}
