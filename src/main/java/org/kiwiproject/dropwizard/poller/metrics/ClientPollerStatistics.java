package org.kiwiproject.dropwizard.poller.metrics;

import org.kiwiproject.dropwizard.poller.ClientPoller;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Defines contract for collecting basic statistics on {@link ClientPoller ) instances.
 */
//TODO: Do we want to enhance this to be stats per host?
public interface ClientPollerStatistics {
    /**
     * Adds a new latency measurement for how long a poll took.
     */
    void addPollLatencyMeasurement(long millis);

    /**
     * The average number of milliseconds it takes per poll.
     */
    double averagePollLatencyInMillis();

    /**
     * Increment the number of attempted polls.
     */
    void incrementCount();

    /**
     * Number of milliseconds since the epoch that the last poll attempt was made.
     */
    Optional<Long> lastAttemptTimeInMillis();

    /**
     * Increment the number of successful polls.
     */
    void incrementSuccessCount();

    /**
     * Increment the number of times polling was skipped.
     */
    void incrementSkipCount();

    /**
     * Number of milliseconds since the epoch that the last poll attempt was skipped.
     */
    Optional<Long> lastSkipTimeInMillis();

    /**
     * Increment the number of failed polls.
     */
    void incrementFailureCount(String message);

    /**
     * Increment the number of failed polls.
     */
    void incrementFailureCount(Throwable exception);

    /**
     * Number of milliseconds since the epoch that the last failure occurred. Intended to be used for time-based
     * health checking of a client poller.
     */
    Optional<Long> lastFailureTimeInMillis();

    /**
     * Returns a {@link Stream) over recent timestamps of poll failures.
     * <p>
     * The implementation determines the limit of the number failures that may be retained and thus returned in the stream.
     * Clients can use stream methods like {@link Stream#limit(long)} to specify a limit of their choosing, which may be
     * smaller or larger than the implementation's internal maximum. See {@link #maxRecentFailureTimes()}.
     */
    Stream<Long> recentFailureTimesInMillis();

    /**
     * Returns a {@link Stream) of Map<String, Object> of error attributes for recent poll failures
     * <p>
     * The implementation should return a number of failure details less than or equal to the number returned from
     * recentFailureTimesInMillis. Clients can use stream methods like {@link Stream#limit(long)} to specify a limit
     * of their choosing, which may be smaller or larger than the implementation's internal maximum.
     * See {@link #maxRecentFailureTimes()}.
     */
    Stream<Map<String, Object>> recentFailureDetails();

    /**
     * Return the maximum number of failure times this implementation supports.
     */
    int maxRecentFailureTimes();

    /**
     * The sum of the incremented success and failure counts.
     */
    int totalCount();

    /**
     * The number of failed polls.
     */
    int failureCount();

    /**
     * The number of successful polls.
     */
    int successCount();

    /**
     * The number of skipped polls.
     */
    int skipCount();

    /**
     * The default class to be used when no {@link ClientPollerStatistics} is specified.
     */
    static Class<? extends ClientPollerStatistics> defaultClass() {
        return DefaultClientPollerStatistics.class;
    }

    /**
     * Factory method to create an instance of the default class for this interface.
     */
    static ClientPollerStatistics newClientPollerStatisticsOfDefaultType() {
        try {
            return defaultClass().getDeclaredConstructor().newInstance();
        } catch (InstantiationException | InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            String message = "Cannot instantiate " + defaultClass().getName();
            throw new IllegalStateException(message, e);
        }
    }
}
