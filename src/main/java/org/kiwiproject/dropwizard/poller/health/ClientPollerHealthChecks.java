package org.kiwiproject.dropwizard.poller.health;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.kiwiproject.base.KiwiStrings.format;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResultBuilder;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheck.Result;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.setup.Environment;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;
import org.kiwiproject.metrics.health.HealthStatus;

import java.net.URI;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Utility class used by the poller health checks that provides common utilities for generating names and results for the health check.
 */
@UtilityClass
public class ClientPollerHealthChecks {

    private static final String UNSPECIFIED_URI_PREFIX = "[unspecified-URI-";
    private static final String UNSPECIFIED_URI_SUFFIX = "]";
    private static final Supplier<String> NAME_FOR_UNSPECIFIED_URI = ClientPollerHealthChecks::nameForUnspecifiedUri;

    @VisibleForTesting
    static final String FAILURE_DETAILS_KEY = "FailureDetails";

    private static String nameForUnspecifiedUri() {
        return UNSPECIFIED_URI_PREFIX + System.currentTimeMillis() + UNSPECIFIED_URI_SUFFIX;
    }

    static String humanReadableOf(TemporalUnit unit) {
        return unit.toString().toLowerCase(Locale.getDefault());
    }

    /**
     * Build a name for the given {@code healthCheckType} and {@code pollingUri}
     *
     * @param healthCheckType The type of the health check
     * @param pollingUri      The polling URI
     * @return a name to represent this health check
     */
    public static String nameFor(String healthCheckType, URI pollingUri) {
        var name = Optional.ofNullable(pollingUri).map(URI::toString).orElseGet(NAME_FOR_UNSPECIFIED_URI);
        return prefixedNameFor(healthCheckType, name);
    }

    /**
     * Build a name for the given {@code healthCheckType} and {@code pollingUri}
     *
     * @param healthCheckType The type of the health check
     * @param pollingUri      The polling URI as a String
     * @return a name to represent this health check
     */
    public static String nameFor(String healthCheckType, String pollingUri) {
        var name = Optional.ofNullable(pollingUri).orElseGet(NAME_FOR_UNSPECIFIED_URI);
        return prefixedNameFor(healthCheckType, name);
    }

    private static String prefixedNameFor(String healthCheckType, String resolvedUri) {
        return format("client-poller-{}:{}", healthCheckType, resolvedUri);
    }

    /**
     * Build an unhealthy {@link Result} using the given statistics,
     * message, and arguments using {@link HealthStatus#WARN} as the severity. The message can be a template, which
     * will be formatted by Dropwizard Metrics using {@link String#format(String, Object...)}.
     *
     * @param statistics      the statistics from which to obtain recent failure details
     * @param messageTemplate the message or message template
     * @param args            arguments for the messageTemplate
     * @return the unhealthy {@link Result}
     */
    public static Result unhealthy(ClientPollerStatistics statistics, String messageTemplate, Object... args) {
        return unhealthy(statistics, HealthStatus.WARN, messageTemplate, args);
    }

    /**
     * Build an unhealthy {@link Result} using the given statistics, severity,
     * message, and arguments. The message can be a template, which will be formatted by Dropwizard Metrics using
     * {@link String#format(String, Object...)}.
     *
     * @param statistics      the statistics from which to obtain recent failure details
     * @param severity        the severity of the result
     * @param messageTemplate the message or message template
     * @param args            arguments for the messageTemplate
     * @return the unhealthy {@link Result}
     */
    public static Result unhealthy(ClientPollerStatistics statistics,
                                   HealthStatus severity,
                                   String messageTemplate,
                                   Object... args) {
        return newUnhealthyResultBuilder(severity)
                .withMessage(messageTemplate, args)
                .withDetail(FAILURE_DETAILS_KEY, statistics.recentFailureDetails().collect(toUnmodifiableList()))
                .build();
    }

    /**
     * Convenience method that registers the following health checks for the given poller and Dropwizard environment:
     * {@link ClientPollerTimeBasedHealthCheck}, {@link ClientPollerLatencyBasedHealthCheck}, and
     * {@link ClientPollerMissedPollHealthCheck}
     *
     * @param poller      The poller being monitored
     * @param environment The dropwizard environment
     * @return a list of health checks that were registered
     */
    public static List<PollerHealthCheck> registerPollerHealthChecks(ClientPoller poller, Environment environment) {
        return registerPollerHealthChecks(poller, environment, PollerHealthCheckConfig.builder().build());
    }

    /**
     * Convenience method that registers the following health checks for the given poller and Dropwizard environment:
     * {@link ClientPollerTimeBasedHealthCheck}, {@link ClientPollerLatencyBasedHealthCheck}, and
     * {@link ClientPollerMissedPollHealthCheck}
     *
     * @param poller            The poller being monitored
     * @param environment       The dropwizard environment
     * @param healthCheckConfig The config for the health checks
     * @return a list of health checks that were registered
     */
    public static List<PollerHealthCheck> registerPollerHealthChecks(ClientPoller poller,
                                                                     Environment environment,
                                                                     PollerHealthCheckConfig healthCheckConfig) {

        var pollerHealthChecks = buildPollerHealthChecks(poller, healthCheckConfig);
        pollerHealthChecks.forEach(healthCheck -> registerPollerHealthCheck(healthCheck, environment));

        return pollerHealthChecks;
    }

    private static void registerPollerHealthCheck(PollerHealthCheck pollerHealthCheck, Environment environment) {
        environment.healthChecks().register(pollerHealthCheck.getName(), pollerHealthCheck.getHealthCheck());
    }

    /**
     * Build the standard client poller health checks, returning a list containing the actual health checks wrapped in a
     * {@link PollerHealthCheck}.
     *
     * @param poller            The poller being monitored
     * @param healthCheckConfig The config for the health checks
     * @return a list of health checks for the poller
     */
    private static List<PollerHealthCheck> buildPollerHealthChecks(ClientPoller poller,
                                                                   PollerHealthCheckConfig healthCheckConfig) {
        var timeBasedHealthCheck = ClientPollerTimeBasedHealthCheck.of(poller, healthCheckConfig);
        var latencyBasedHealthCheck = ClientPollerLatencyBasedHealthCheck.of(poller, healthCheckConfig);
        var missedPollHealthCheck = ClientPollerMissedPollHealthCheck.of(poller, healthCheckConfig);

        return List.of(
                new PollerHealthCheck(ClientPollerTimeBasedHealthCheck.nameFor(poller.getName()), timeBasedHealthCheck),
                new PollerHealthCheck(ClientPollerLatencyBasedHealthCheck.nameFor(poller.getName()), latencyBasedHealthCheck),
                new PollerHealthCheck(ClientPollerMissedPollHealthCheck.nameFor(poller.getName()), missedPollHealthCheck)
        );
    }

    @AllArgsConstructor
    @Getter
    public static class PollerHealthCheck {
        private final String name;
        private final HealthCheck healthCheck;
    }
}
