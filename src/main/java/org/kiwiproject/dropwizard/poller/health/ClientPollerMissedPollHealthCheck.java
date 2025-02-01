package org.kiwiproject.dropwizard.poller.health;

import static com.google.common.base.Preconditions.checkArgument;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;
import static org.kiwiproject.time.KiwiDurationFormatters.formatMillisecondDurationWords;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.base.DefaultEnvironment;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;

import java.net.URI;
import java.util.Optional;

/**
 * Health check that monitors and alerts if missed polls grows beyond an acceptable threshold
 */
@Slf4j
public class ClientPollerMissedPollHealthCheck extends HealthCheck {

    /**
     * The default multiplier for computing the allowable time since the last attempted poll.
     */
    public static final int DEFAULT_MISSING_POLL_MULTIPLIER = 10;

    private static final String TYPE = "missedPoll";
    private static final long NO_LAST_ATTEMPT_MILLIS = 0L;

    private final ClientPollerStatistics statistics;

    @Getter
    private final int missingPollMultiplier;

    @Getter
    private final long missingPollAlertThresholdMillis;

    @VisibleForTesting
    KiwiEnvironment kiwiEnvironment = new DefaultEnvironment();

    private ClientPollerMissedPollHealthCheck(ClientPollerStatistics statistics,
                                              ClientPoller poller,
                                              Integer missingPollMultiplier) {
        this.statistics = requireNotNull(statistics, "statistics cannot be null");
        this.missingPollMultiplier = Optional.ofNullable(missingPollMultiplier).orElse(DEFAULT_MISSING_POLL_MULTIPLIER);

        checkArgument(this.missingPollMultiplier > 0, "missingPollMultiplier must be positive");
        checkArgumentNotNull(poller, "poller cannot be null");

        this.missingPollAlertThresholdMillis = poller.getExecutionInterval() * this.missingPollMultiplier;

        LOG.info("For poller with execution interval {} and missing poll multiplier {}," +
                        " using {} as the missing poll alert threshold, after which we will report the poller unhealthy",
                formatMillisecondDurationWords(poller.getExecutionInterval()),
                this.missingPollMultiplier,
                formatMillisecondDurationWords(missingPollAlertThresholdMillis));
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poller with the given
     * {@link PollerHealthCheckConfig}
     *
     * @param poller            The poller that the health check is monitoring
     * @param healthCheckConfig The config for the health checks
     * @return a new instance of this health check
     */
    public static ClientPollerMissedPollHealthCheck of(ClientPoller poller, PollerHealthCheckConfig healthCheckConfig) {
        return of(poller, healthCheckConfig.getMissingPollMultiplier());
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poller with the default multiplier
     *
     * @param poller The poller that the health check is monitoring
     * @return a new instance of this health check
     */
    public static ClientPollerMissedPollHealthCheck of(ClientPoller poller) {
        return of(poller, DEFAULT_MISSING_POLL_MULTIPLIER);
    }

    /**
     * Create a new {@link ClientPollerMissedPollHealthCheck} for the given poller with the given multiplier
     *
     * @param poller                The poller that the health check is monitoring
     * @param missingPollMultiplier The multiplier to use for the health check
     * @return a new instance of this health check
     */
    public static ClientPollerMissedPollHealthCheck of(ClientPoller poller, Integer missingPollMultiplier) {
        checkArgumentNotNull(poller);
        return new ClientPollerMissedPollHealthCheck(poller.statistics(), poller, missingPollMultiplier);
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

    @Override
    protected Result check() {
        long lastAttemptMillis = statistics.lastAttemptTimeInMillis().orElse(NO_LAST_ATTEMPT_MILLIS);
        long lastSkipMillis = statistics.lastSkipTimeInMillis().orElse(NO_LAST_ATTEMPT_MILLIS);

        if (lastAttemptMillis == NO_LAST_ATTEMPT_MILLIS && lastSkipMillis == NO_LAST_ATTEMPT_MILLIS) {
            return ClientPollerHealthChecks.unhealthy(statistics, "There is no last poll attempt or skipped poll time");
        }

        var now = kiwiEnvironment.currentTimeMillis();
        var lastAttemptType = LastAttemptType.determine(lastAttemptMillis, lastSkipMillis);
        var lastAttemptOrSkip = LastAttemptType.chooseLastAttemptOrSkipMillis(lastAttemptType, lastAttemptMillis, lastSkipMillis);
        var delta = now - lastAttemptOrSkip;

        if (delta < 0) {
            LOG.warn("Negative time difference since last {}, which could indicate a system clock problem", lastAttemptType.description);
            return ClientPollerHealthChecks.unhealthy(
                    statistics,
                    "Negative time difference since last %s (current time millis is %d; last attempt was %d; delta millis is %d",
                    lastAttemptType.description,
                    now,
                    lastAttemptOrSkip,
                    delta);
        }

        if (delta > missingPollAlertThresholdMillis) {
            return ClientPollerHealthChecks.unhealthy(
                    statistics,
                    "Time since last %s (%d millis) exceeds alert threshold (%d millis)",
                    lastAttemptType.description,
                    delta,
                    missingPollAlertThresholdMillis);
        }

        return newHealthyResult("Time since last %s is %d millis (threshold is %d millis)",
                lastAttemptType.description, delta, missingPollAlertThresholdMillis);
    }

    enum LastAttemptType {
        POLL("poll"), SKIP("skip");

        final String description;

        LastAttemptType(String description) {
            this.description = description;
        }

        static LastAttemptType determine(long lastAttemptMillis, long lastSkipMillis) {
            if (lastAttemptMillis > lastSkipMillis) {
                return POLL;
            }

            return SKIP;
        }

        static long chooseLastAttemptOrSkipMillis(LastAttemptType type, long lastAttemptMillis, long lastSkipMillis) {
            if (type == POLL) {
                return lastAttemptMillis;
            }

            return lastSkipMillis;
        }
    }
}
