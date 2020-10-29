package org.kiwiproject.dropwizard.poller.metrics;

import static java.util.Objects.nonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.collect.KiwiEvictingQueues;
import org.kiwiproject.collect.KiwiMaps;
import org.kiwiproject.concurrent.TryLocker;
import org.kiwiproject.dropwizard.poller.ClientPoller;
import org.kiwiproject.time.KiwiDateTimeFormatters;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * The default implementation of {@link ClientPollerStatistics}.
 * <p>
 * This implementation is <em>effectively thread-safe<em> if each {@link ClientPoller} is using its own
 * {@link ClientPollerStatistics} instance. This is true because a poller instance is scheduled to execute at a
 * fixed-rate and so it sleeps, executes, sleeps, executes, etc. Thus there is only <em>one</em> poller instance acting
 * on its {@link ClientPollerStatistics} instance at any given time.
 */
@Slf4j
public class DefaultClientPollerStatistics implements ClientPollerStatistics {
    @VisibleForTesting
    static final int MAX_RECENT_FAILURES = 100;

    private static final int FAILURE_DETAIL_LIMIT = 7;
    private final AtomicInteger count;
    private final AtomicLong lastAttemptTime;
    private final AtomicInteger successCount;
    private final AtomicInteger skipCount;
    private final AtomicLong lastSkipTime;
    private final AtomicInteger failureCount;
    private final AtomicLong lastFailureTime;
    private final Queue<FailedPollResult> recentFailures;
    private final TryLocker latencyLocker;
    private final AtomicInteger numberOfPollLatencyMeasurements;
    private final AtomicDouble avgPollLatency;

    public DefaultClientPollerStatistics() {
        this(TryLocker.usingReentrantLock());
    }

    @VisibleForTesting
    DefaultClientPollerStatistics(TryLocker latencyTryLocker) {
        count = new AtomicInteger(0);
        lastAttemptTime = new AtomicLong(0);
        successCount = new AtomicInteger(0);
        skipCount = new AtomicInteger(0);
        lastSkipTime = new AtomicLong(0);
        failureCount = new AtomicInteger(0);
        lastFailureTime = new AtomicLong(0);
        recentFailures = KiwiEvictingQueues.synchronizedEvictingQueue(MAX_RECENT_FAILURES);
        numberOfPollLatencyMeasurements = new AtomicInteger(0);
        avgPollLatency = new AtomicDouble(0.0);
        latencyLocker = latencyTryLocker;
    }

    /**
     * @implNote This is left public for now, so we can expose it with new methods, e.g. one that returns a
     * {@code Stream<FailedPollResult>}.
     */
    @SuppressWarnings("WeakerAccess")
    public static class FailedPollResult {
        public static final String TIME_KEY = "timeMillis";
        public static final String TIME_STRING_KEY = "timeString";
        public static final String EXCEPTION_TYPE_KEY = "exceptionType";
        public static final String EXCEPTION_MESSAGE_KEY = "exceptionMessage";
        public static final String MESSAGE_KEY = "message";

        @Getter
        private final long time;

        private final Throwable exception;
        private final String message;

        FailedPollResult(Throwable exception) {
            time = now();
            this.exception = exception;
            this.message = null;
        }

        FailedPollResult(String message) {
            time = now();
            this.exception = null;
            this.message = message;
        }

        public Map<String, Object> getDetails() {
            Map<String, Object> details = KiwiMaps.newHashMap(TIME_KEY, time,
                    TIME_STRING_KEY, humanReadableDateTime(time));
            if (nonNull(exception)) {
                details.put(EXCEPTION_TYPE_KEY, exception.getClass().getSimpleName());
                details.put(EXCEPTION_MESSAGE_KEY, exception.getMessage());
            }
            if (nonNull(message)) {
                details.put(MESSAGE_KEY, message);
            }
            return details;
        }

        private static String humanReadableDateTime(long time) {
            var dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC);
            return KiwiDateTimeFormatters.formatAsIsoZonedDateTimeUTC(dateTime);
        }
    }

    /**
     * See section "Cumulative moving average" (CMA) on Moving Average page: https://en.wikipedia.org/wiki/Moving_average
     */
    @Override
    public void addPollLatencyMeasurement(long latencyInMillis) {
        latencyLocker.withLockOrElse(
                () -> calculateAndSetNewCumulativeMovingAverage(latencyInMillis),
                () -> logSkippedMeasurement(latencyInMillis));
    }

    private void calculateAndSetNewCumulativeMovingAverage(long latencyInMillis) {
        int numMeasurements = numberOfPollLatencyMeasurements.getAndIncrement();
        double cmaOrig = avgPollLatency.get();
        double cmaNew = cumulativeMovingAverage(cmaOrig, latencyInMillis, numMeasurements);
        avgPollLatency.set(cmaNew);
    }

    private static double cumulativeMovingAverage(double cmaOrig, long newValue, int numValues) {
        return cmaOrig + ((newValue - cmaOrig) / (numValues + 1));
    }

    private void logSkippedMeasurement(long latencyInMillis) {
        LOG.warn("Skipping measurement {} because lock was not obtained in {} {}",
                latencyInMillis, latencyLocker.getLockWaitTime(), latencyLocker.getLockWaitTimeUnit());
    }

    @Override
    public double averagePollLatencyInMillis() {
        return avgPollLatency.get();
    }

    @Override
    public void incrementCount() {
        count.incrementAndGet();
        lastAttemptTime.set(now());
    }

    @Override
    public Optional<Long> lastAttemptTimeInMillis() {
        long currentValue = lastAttemptTime.get();
        return valueIfNonZeroOrElseEmpty(currentValue);
    }

    @Override
    public void incrementSuccessCount() {
        successCount.incrementAndGet();
    }

    @Override
    public void incrementSkipCount() {
        skipCount.incrementAndGet();
        lastSkipTime.set(now());
    }

    @Override
    public Optional<Long> lastSkipTimeInMillis() {
        long currentValue = lastSkipTime.get();
        return valueIfNonZeroOrElseEmpty(currentValue);
    }

    @Override
    public void incrementFailureCount(String message) {
        incrementFailureCount();
        recentFailures.add(new FailedPollResult(message));
    }

    @Override
    public void incrementFailureCount(Throwable exception) {
        incrementFailureCount();
        recentFailures.add(new FailedPollResult(exception));
    }

    private void incrementFailureCount() {
        failureCount.incrementAndGet();
        lastFailureTime.set(now());
    }

    private static long now() {
        return System.currentTimeMillis();
    }

    @Override
    public Optional<Long> lastFailureTimeInMillis() {
        long currentValue = lastFailureTime.get();
        return valueIfNonZeroOrElseEmpty(currentValue);
    }

    private static Optional<Long> valueIfNonZeroOrElseEmpty(long currentValue) {
        if (currentValue == 0) {
            return Optional.empty();
        }
        return Optional.of(currentValue);
    }

    @Override
    public Stream<Long> recentFailureTimesInMillis() {
        return recentFailures.stream().map(FailedPollResult::getTime);
    }

    @Override
    public Stream<Map<String, Object>> recentFailureDetails() {
        int skipFirstNElements = recentFailures.size() - numberOfFailureDetails();
        return recentFailures.stream()
                .skip(skipFirstNElements)
                .map(FailedPollResult::getDetails);
    }

    @VisibleForTesting
    int numberOfFailureDetails() {
        return recentFailures.size() > FAILURE_DETAIL_LIMIT ? FAILURE_DETAIL_LIMIT : recentFailures.size();
    }

    @Override
    public int maxRecentFailureTimes() {
        return MAX_RECENT_FAILURES;
    }

    @Override
    public int totalCount() {
        return count.get();
    }

    @Override
    public int successCount() {
        return successCount.get();
    }

    @Override
    public int skipCount() {
        return skipCount.get();
    }

    @Override
    public int failureCount() {
        return failureCount.get();
    }
}