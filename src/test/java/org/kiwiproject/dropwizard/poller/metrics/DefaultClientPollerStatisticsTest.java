package org.kiwiproject.dropwizard.poller.metrics;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kiwiproject.concurrent.TryLocker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.IntStream;

@DisplayName("DefaultClientPollerStatistics")
@ExtendWith(SoftAssertionsExtension.class)
class DefaultClientPollerStatisticsTest {

    private DefaultClientPollerStatistics stats;

    @BeforeEach
    void setUp() {
        stats = new DefaultClientPollerStatistics();
    }

    @Test
    void testInitialState(SoftAssertions softly) {
        softly.assertThat(stats.totalCount()).isZero();
        softly.assertThat(stats.successCount()).isZero();
        softly.assertThat(stats.skipCount()).isZero();
        softly.assertThat(stats.lastSkipTimeInMillis()).isEmpty();
        softly.assertThat(stats.failureCount()).isZero();
        softly.assertThat(stats.lastFailureTimeInMillis()).isEmpty();
        softly.assertThat(stats.recentFailureTimesInMillis().collect(toList())).isEmpty();
        softly.assertThat(stats.recentFailureDetails().collect(toList())).isEmpty();
    }

    @Test
    void testIncrementCount_SetsLastAttemptTime() {
        stats.incrementCount();

        assertThat(stats.totalCount()).isOne();
        assertThat(stats.lastAttemptTimeInMillis())
                .isPresent()
                .hasValueSatisfying(this::justOccurred);
    }

    @Test
    void testIncrementSkipCount_SetsLastSkipTime() {
        stats.incrementSkipCount();

        assertThat(stats.totalCount()).isZero();
        assertThat(stats.skipCount()).isOne();
        assertThat(stats.lastSkipTimeInMillis())
                .isPresent()
                .hasValueSatisfying(this::justOccurred);
    }

    private void justOccurred(long timeInMillis) {
        long now = System.currentTimeMillis();
        assertThat(timeInMillis)
                .isLessThanOrEqualTo(now)
                .isGreaterThan(now - TimeUnit.SECONDS.toMillis(1));
    }

    @Test
    void testWhenThereAreNoFailures(SoftAssertions softly) {
        IntStream.range(0, 25).forEach(value -> stats.incrementSuccessCount());

        softly.assertThat(stats.lastFailureTimeInMillis()).isEmpty();
        softly.assertThat(stats.recentFailureTimesInMillis().count()).isZero();
        softly.assertThat(stats.recentFailureDetails().count()).isZero();
    }

    @Test
    void testLastFailureMillis() {
        long time1 = System.currentTimeMillis();
        stats.incrementFailureCount(new Exception("error1"));
        IntStream.range(0, 1_000).forEach(value -> stats.incrementSuccessCount());

        long time2 = System.currentTimeMillis();
        assertThat(stats.lastFailureTimeInMillis().orElseThrow(IllegalStateException::new)).isBetween(time1, time2);

        stats.incrementFailureCount(new Exception("error2"));
        assertThat(stats.lastFailureTimeInMillis().orElseThrow(IllegalStateException::new)).isGreaterThanOrEqualTo(time2);
    }

    @Test
    void testStreamOfRecentFailureTimesInMillis_DoesNotRemoveElements() {
        var twiceMaxRecentFailureTimes = 2 * stats.maxRecentFailureTimes();
        createFailures(stats, twiceMaxRecentFailureTimes);

        stats.recentFailureTimesInMillis().forEach(this::noop);
        assertThat(stats.recentFailureTimesInMillis().count()).isEqualTo(stats.maxRecentFailureTimes());
    }

    private void noop(long value) {
    }

    static void createFailures(ClientPollerStatistics stats, int numberOfFailures) {
        IntStream.range(0, numberOfFailures)
                .forEach(value -> stats.incrementFailureCount(new Exception(f("error{}", value))));
    }

    @Test
    void testStreamOfRecentFailureTimesInMillis_UpToMaxCapacity() {
        var time1 = System.currentTimeMillis();
        createFailures(stats, stats.maxRecentFailureTimes());

        var time2 = System.currentTimeMillis();

        stats.recentFailureTimesInMillis().forEach(time -> assertThat(time).isBetween(time1, time2));
    }

    @Test
    void testStreamOfRecentFailureTimesInMillis_BeyondMaxCapacity() {
        var time1 = System.currentTimeMillis();

        createFailures(stats, stats.maxRecentFailureTimes());

        var time2 = System.currentTimeMillis();
        assertThat(time2).isGreaterThanOrEqualTo(time1);

        createFailures(stats, stats.maxRecentFailureTimes());
        var time3 = System.currentTimeMillis();

        stats.recentFailureTimesInMillis().forEach(time -> assertThat(time).isBetween(time2, time3));
    }

    @Test
    void testRecentFailureDetails_LimitsNumberOfRecentFailureDetails(SoftAssertions softly) {
        createFailures(stats, stats.maxRecentFailureTimes());

        var expectedNumberOfFailureDetails = stats.numberOfFailureDetails();

        List<Map<String, Object>> failureDetails = stats.recentFailureDetails().collect(toList());
        Collections.reverse(failureDetails);

        softly.assertThat(failureDetails).hasSize(expectedNumberOfFailureDetails);

        List<Long> recentFailureTimes = stats.recentFailureTimesInMillis().collect(toList());
        Collections.reverse(recentFailureTimes);

        // noinspection UnstableApiUsage
        Streams.zip(failureDetails.stream(), recentFailureTimes.stream(),
                        (failureDetail, failureTime) -> Pair.of(failureDetail.get(DefaultClientPollerStatistics.FailedPollResult.TIME_KEY), failureTime))
                .forEach(pair ->
                        softly.assertThat(pair.getLeft())
                                .describedAs("Failure detail time must match failure time")
                                .isEqualTo(pair.getRight()));

    }

    @Test
    void testRecentFailureDetails_NullExceptionHandled(SoftAssertions softly) {
        stats.incrementFailureCount(new Exception("error1"));
        stats.incrementFailureCount("NoException1");
        stats.incrementFailureCount(new Exception("error2"));
        stats.incrementFailureCount("NoException2");

        softly.assertThat(stats.failureCount()).isEqualTo(4);

        List<Map<String, Object>> failureDetails = stats.recentFailureDetails().collect(toList());

        softly.assertThat(failureDetails).hasSize(4);
    }

    @Test
    void testCumulativeAveragePollLatency_IsInitiallyZero() {
        assertThat(stats.averagePollLatencyInMillis()).isZero();
    }

    @Test
    void testCumulativeAveragePollLatency_WhenLockIsNotObtained_ShouldSkipMeasurement() throws InterruptedException {
        var mockLock = mock(Lock.class);
        var locker = TryLocker.using(mockLock, 1, TimeUnit.MILLISECONDS);

        stats = new DefaultClientPollerStatistics(locker);

        when(mockLock.tryLock(anyLong(), any(TimeUnit.class)))
                .thenReturn(false)
                .thenReturn(false)
                .thenReturn(true);

        stats.addPollLatencyMeasurement(200);
        stats.addPollLatencyMeasurement(100);
        stats.addPollLatencyMeasurement(500);

        assertThat(stats.averagePollLatencyInMillis()).isCloseTo(500.0, within(1E-6));

        verify(mockLock, times(3)).tryLock(1, TimeUnit.MILLISECONDS);
    }

    @Test
    void testCumulativeAveragePollLatency_ForOneValue_EqualsItself() {
        stats.addPollLatencyMeasurement(420);
        assertThat(stats.averagePollLatencyInMillis()).isCloseTo(420.0, within(1E-6));
    }

    @Test
    void testCumulativeAveragePollLatency_ForTwoSetsOfValues() {
        for (int i = 0; i < 100; i++) {
            stats.addPollLatencyMeasurement(100);
            stats.addPollLatencyMeasurement(200);
        }

        assertThat(stats.averagePollLatencyInMillis()).isCloseTo(150.0, within(1E-6));
    }

    @Test
    void testCumulativeAveragePollLatency_ForRandomValues() {
        var random = new Random();
        var randomLatencies = new ArrayList<Long>();

        for (int i = 0; i < 1_000; i++) {
            long value = random.nextInt(1000);
            randomLatencies.add(value);
            stats.addPollLatencyMeasurement(value);
        }

        var expectedStats = calculateExpectedLatencyStats(randomLatencies);
        assertThat(stats.averagePollLatencyInMillis()).isCloseTo(expectedStats.getAverage(), within(1E-6));
    }

    @Test
    void testCumulativeAveragePollLatency_ForManyRandomValues_UsingMultipleThreads() {
        var randomLatencies = Collections.synchronizedList(new ArrayList<Long>());
        var parallelStream = IntStream.range(0, 20_000).parallel();

        parallelStream.forEach(ignoredIndex -> {
            var value = ThreadLocalRandom.current().nextLong(1000);
            randomLatencies.add(value);
            stats.addPollLatencyMeasurement(value);
        });

        var expectedStats = calculateExpectedLatencyStats(randomLatencies);

        assertThat(stats.averagePollLatencyInMillis()).isCloseTo(expectedStats.getAverage(), within(1E-6));
    }

    private LongSummaryStatistics calculateExpectedLatencyStats(List<Long> randomLatencies) {
        return randomLatencies.stream().collect(
                LongSummaryStatistics::new,
                LongSummaryStatistics::accept,
                LongSummaryStatistics::combine);
    }
}
