package org.kiwiproject.dropwizard.poller;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.FIVE_SECONDS;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.core.setup.Environment;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.SyncInvoker;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck;
import org.kiwiproject.dropwizard.poller.health.ClientPollerMissedPollHealthCheck;
import org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;
import org.kiwiproject.dropwizard.poller.metrics.DefaultClientPollerStatistics;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

@DisplayName("ClientPoller")
@ExtendWith(SoftAssertionsExtension.class)
@Slf4j
class ClientPollerTest {

    private SyncInvoker invoker;
    private Consumer<Response> consumer;
    private Response response;
    private long intervalInMillis;
    private ClientPoller poller;
    private ScheduledThreadPoolExecutor executor;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        invoker = mock(SyncInvoker.class);
        consumer = mock(Consumer.class);

        response = mock(Response.class);
        when(response.getStatus()).thenReturn(200);

        intervalInMillis = 50;
        executor = new ScheduledThreadPoolExecutor(1);

        poller = buildAsyncPoller(clientPollerStatistics -> true);
    }

    @AfterEach
    void tearDown() {
        poller.stop();
    }

    private ClientPoller buildAsyncPoller(Function<ClientPollerStatistics, Boolean> decisionFn) {
        return buildPoller(ConsumerType.ASYNC, decisionFn);
    }

    private ClientPoller buildSyncPoller(Function<ClientPollerStatistics, Boolean> decisionFn) {
        return buildPoller(ConsumerType.SYNC, decisionFn);
    }

    private ClientPoller buildPoller(ConsumerType consumerType, Function<ClientPollerStatistics, Boolean> decisionFn) {
        return ClientPoller.builder()
                .name("Test Poller")
                .supplier(() -> invoker)
                .decisionFunction(decisionFn)
                .consumerType(consumerType)
                .consumer(consumer)
                .executor(executor)
                .initialExecutionDelay(Duration.ZERO)
                .executionInterval(intervalInMillis)
                .build();
    }

    @Test
    void pollersDefaultToAsync(SoftAssertions softly) {
        var asyncPoller = ClientPoller.builder()
                .name("Test Poller")
                .consumer(consumer)
                .supplier(() -> invoker)
                .executor(executor)
                .executionInterval(intervalInMillis)
                .build();

        try {
            when(invoker.get()).thenReturn(response);

            asyncPoller.start();

            softly.assertThat(asyncPoller.isAsync()).isTrue();
            softly.assertThat(asyncPoller.getName()).isEqualTo("Test Poller");
            softly.assertThat(asyncPoller.getSupplierTimeout()).isEqualTo(ClientPoller.DEFAULT_SUPPLIER_TIMEOUT);
            softly.assertThat(asyncPoller.getSupplierTimeoutUnit()).isEqualTo(ClientPoller.DEFAULT_SUPPLIER_TIMEOUT_UNIT);
            softly.assertThat(asyncPoller.statistics()).isNotNull();
        } finally {
            asyncPoller.stop();
        }
    }

    @Test
    void testDefaultValues_ForSyncPoller(SoftAssertions softly) {
        var syncPoller = ClientPoller.builder()
                .name("Test Synchronous Poller")
                .consumerType(ConsumerType.SYNC)
                .consumer(consumer)
                .supplier(() -> invoker)
                .executor(executor)
                .executionInterval(intervalInMillis)
                .build();

        try {
            when(invoker.get()).thenReturn(response);

            syncPoller.start();

            softly.assertThat(syncPoller.isAsync()).isFalse();
            softly.assertThat(syncPoller.getName()).isEqualTo("Test Synchronous Poller");
            softly.assertThat(syncPoller.statistics()).isNotNull();
            softly.assertThat(syncPoller.getSupplierTimeout()).isEqualTo(ClientPoller.DEFAULT_SUPPLIER_TIMEOUT);
            softly.assertThat(syncPoller.getSupplierTimeoutUnit()).isEqualTo(ClientPoller.DEFAULT_SUPPLIER_TIMEOUT_UNIT);
            softly.assertThat(syncPoller.getSyncConsumerTimeout()).isEqualTo(ClientPoller.DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT);
            softly.assertThat(syncPoller.getSyncConsumerTimeoutUnit()).isEqualTo(ClientPoller.DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT_UNIT);
        } finally {
            syncPoller.stop();
        }
    }

    @Test
    void testPollersGetADefault_Name_IfNotSupplied() {
        poller = ClientPoller.builder()
                .consumer(consumer)
                .supplier(() -> invoker)
                .executor(executor)
                .executionInterval(intervalInMillis)
                .build();

        assertThat(poller.getName()).matches("^Poller_[0-9]{13}");
    }

    @Nested
    class Validation {

        private ClientPoller validationPoller;
        private ScheduledExecutorService scheduledExecutor;

        @BeforeEach
        void setUp() {
            scheduledExecutor = mock(ScheduledExecutorService.class);
        }

        @AfterEach
        void tearDown() {
            if (nonNull(validationPoller)) {
                validationPoller.stop();
            }
        }

        @Test
        void shouldRequireSupplier() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("supplier cannot be null");
        }

        @Test
        void shouldRequireConsumerType() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .consumerType(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("consumerType cannot be null");
        }

        @Test
        void shouldRequireSyncConsumerTimeout_WhenSyncPoller() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .consumerType(ConsumerType.SYNC)
                    .syncConsumerTimeout(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("syncConsumerTimeout cannot be null for sync poller");
        }

        @ParameterizedTest
        @ValueSource(longs = {-10, -5, -1, 0})
        void shouldRequirePositiveSyncConsumerTimeout_WhenSyncPoller(long timeout) {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .consumerType(ConsumerType.SYNC)
                    .syncConsumerTimeout(timeout)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("syncConsumerTimeout must be greater than zero");
        }

        @Test
        void shouldRequireSyncConsumerTimeoutUnit_WhenSyncPoller() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .consumerType(ConsumerType.SYNC)
                    .syncConsumerTimeoutUnit(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("syncConsumerTimeoutUnit cannot be null for sync poller");
        }

        @Test
        void shouldRequireSupplierTimeout() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .supplierTimeout(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("supplierTimeout cannot be null");
        }

        @ParameterizedTest
        @ValueSource(longs = {-10, -5, -1, 0})
        void shouldRequirePositiveSupplierTimeout(long timeout) {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .supplierTimeout(timeout)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("supplierTimeout must be greater than zero");
        }

        @Test
        void shouldRequireSupplierTimeoutUnit() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .supplierTimeoutUnit(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("supplierTimeoutUnit cannot be null");
        }

        @Test
        void shouldRequireInitialExecutionDelay() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .initialExecutionDelay(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("initialExecutionDelay cannot be null");
        }

        @Test
        void shouldRequireConsumer() {
            validationPoller = ClientPoller.builder()
                    .supplier(() -> invoker)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("consumer cannot be null");
        }

        @Test
        void shouldRequireDecisionFunction() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .decisionFunction(null)
                    .executor(scheduledExecutor)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("decisionFunction cannot be null");
        }

        @ParameterizedTest
        @ValueSource(longs = {-10, -1, 0})
        void shouldRequirePositiveExecutionInterval(long interval) {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .executionInterval(interval)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("executionInterval must be a positive number of milliseconds!");
        }

        @Test
        void shouldRequireExecutor() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .executionInterval(5_000)
                    .build();

            assertThrowsIllegalStateExceptionWithMessage("executor cannot be null");
        }

        @Test
        void shouldSetDefaultConsumerExecutor_WhenNullIsExplicitlyPassed() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .executionInterval(5_000)
                    .executor(scheduledExecutor)
                    .consumerExecutor(null)
                    .build();

            assertThatCode(() -> validationPoller.start()).doesNotThrowAnyException();
        }

        @Test
        void shouldAcceptCustomConsumerExecutor() {
            validationPoller = ClientPoller.builder()
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .executionInterval(5_000)
                    .executor(scheduledExecutor)
                    .consumerExecutor(Executors.newSingleThreadExecutor())
                    .build();

            assertThatCode(() -> validationPoller.start()).doesNotThrowAnyException();
        }

        private void assertThrowsIllegalStateExceptionWithMessage(String message) {
            assertThatThrownBy(() -> validationPoller.start())
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage(message);
        }
    }

    @Nested
    class DifficultToTestMethods {

        @Nested
        class UpdateUri {

            private ClientPoller poller;
            private PollerSyncInvokerWrapper wrappedInvoker;

            @BeforeEach
            void setUp() {
                wrappedInvoker = PollerSyncInvokerWrapper.builder()
                        .delegateSyncInvoker(invoker)
                        .uri("https://localhost:10042")
                        .build();

                poller = ClientPoller.builder()
                        .consumer(consumer)
                        .supplier(() -> wrappedInvoker)
                        .executor(executor)
                        .executionInterval(intervalInMillis)
                        .build();
            }

            @Test
            void shouldUpdateUri_WhenGivenWrapper() {
                var uri = poller.updateUri(wrappedInvoker);
                assertThat(uri)
                        .hasScheme("https")
                        .hasAuthority("localhost:10042")
                        .hasNoQuery();
            }

            @Test
            void shouldNotUpdateUri_WhenGivenArgument_ThatIsNotWrapper() {
                var uri = poller.updateUri(invoker);
                assertThat(uri).isNull();
            }
        }

        @Nested
        class WaitForCompletion {

            private ClientPoller syncPoller;
            private CompletableFuture<Void> future;

            @BeforeEach
            void setUp() {
                var wrappedInvoker = PollerSyncInvokerWrapper.builder()
                        .delegateSyncInvoker(invoker)
                        .uri("https://localhost:10000")
                        .build();

                syncPoller = ClientPoller.builder()
                        .name("Test Synchronous Poller")
                        .consumerType(ConsumerType.SYNC)
                        .consumer(consumer)
                        .supplier(() -> wrappedInvoker)
                        .executor(executor)
                        .executionInterval(intervalInMillis)
                        .build();

                future = mock();
            }

            @AfterEach
            void tearDown() {
                // If the current thread is interrupted, then reset the interrupted status by the following
                var result = Thread.interrupted();
                if (result) {
                    LOG.info("Thread interrupted status cleared");
                }
            }

            @Test
            void shouldCatchInterruptedExceptions() throws InterruptedException, ExecutionException, TimeoutException {
                when(future.get(anyLong(), any(TimeUnit.class)))
                        .thenThrow(new InterruptedException("How rude!"));

                assertThatCode(() -> syncPoller.waitForCompletion(future)).doesNotThrowAnyException();
            }

            @Test
            void shouldInterruptAfterCatchingInterruptedExceptions() throws InterruptedException, ExecutionException, TimeoutException {
                when(future.get(anyLong(), any(TimeUnit.class)))
                        .thenThrow(new InterruptedException("How rude!"));

                syncPoller.waitForCompletion(future);

                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            }

            @Test
            void shouldCatchExecutionExceptions() throws InterruptedException, ExecutionException, TimeoutException {
                when(future.get(anyLong(), any(TimeUnit.class)))
                        .thenThrow(new ExecutionException("error executing", new RuntimeException("the cause")));

                assertThatCode(() -> syncPoller.waitForCompletion(future)).doesNotThrowAnyException();
            }

            @Test
            void shouldCatchTimeoutExceptions() throws InterruptedException, ExecutionException, TimeoutException {
                when(future.get(anyLong(), any(TimeUnit.class)))
                        .thenThrow(new TimeoutException("operation timed out!"));

                assertThatCode(() -> syncPoller.waitForCompletion(future)).doesNotThrowAnyException();
            }
        }

        @Nested
        class ShutdownQuietly {

            private String name;
            private ExecutorService executorService;

            @BeforeEach
            void setUp() {
                name = "myExecutor";
                executorService = mock(ExecutorService.class);
            }

            @Test
            void shouldIgnoreNullArguments() {
                assertThatCode(() -> ClientPoller.shutdownQuietly(null, null)).doesNotThrowAnyException();
            }

            @Test
            void shouldIgnoreExceptionsThrownByExecutorShutdown() throws InterruptedException {
                doThrow(new RuntimeException("What happened here?")).when(executorService).shutdown();

                assertThatCode(() -> ClientPoller.shutdownQuietly(name, executorService)).doesNotThrowAnyException();

                verify(executorService).shutdown();
                verify(executorService, never()).awaitTermination(anyLong(), any(TimeUnit.class));
            }

            @Test
            void shouldIgnoreExceptionsThrownByExecutorShutdown_WhenAwaitingTermination() throws InterruptedException {
                when(executorService.awaitTermination(anyLong(), any(TimeUnit.class)))
                        .thenThrow(new InterruptedException("You're done NOW mister!"));

                assertThatCode(() -> ClientPoller.shutdownQuietly(name, executorService)).doesNotThrowAnyException();

                verify(executorService).shutdown();
                verify(executorService).awaitTermination(5L, TimeUnit.SECONDS);
            }

            @Test
            void shouldIgnoreFailureToTerminateWithinAllottedTime_WhenAwaitingTermination() throws InterruptedException {
                when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(false);

                assertThatCode(() -> ClientPoller.shutdownQuietly(name, executorService)).doesNotThrowAnyException();

                verify(executorService).shutdown();
                verify(executorService).awaitTermination(5L, TimeUnit.SECONDS);
            }
        }

        @Nested
        class UncaughtExceptionHandler {

            @Test
            void shouldIncrementErrorCount() {
                var stats = new DefaultClientPollerStatistics();
                var handler = ClientPoller.newUncaughtExceptionHandler(stats);

                assertThat(stats.failureCount()).isZero();

                handler.uncaughtException(Thread.currentThread(), new RuntimeException("Don't get technical with me!"));
                assertThat(stats.failureCount()).isOne();
            }
        }

        @Nested
        class IsPolling {

            @Test
            void shouldTransitionCorrectlyDuringPollerLifecycle(SoftAssertions softly) {
                when(invoker.get()).thenReturn(response);

                var thePoller = buildSyncPoller(statistics -> true);

                softly.assertThat(thePoller.isPolling())
                        .describedAs("should not be polling before start is called")
                        .isFalse();

                thePoller.start();

                softly.assertThat(thePoller.isPolling())
                        .describedAs("should be polling after start is called (and before stop is called)")
                        .isTrue();

                try {
                    Callable<Boolean> condition = () -> thePoller.statistics().totalCount() > 3;
                    await().atMost(FIVE_SECONDS).until(condition);
                } catch (Exception e) {
                    System.out.println(thePoller.statistics().totalCount());
                    fail(e.getMessage());
                } finally {
                    softly.assertThat(thePoller.isPolling())
                            .describedAs("should still be polling right before stop is called")
                            .isTrue();

                    thePoller.stop();
                }

                softly.assertThat(thePoller.isPolling())
                        .describedAs("should not be polling after stop is called")
                        .isFalse();
            }
        }

        @Test
        void testRegisterHealthChecks_UsingFluentApiMethod_AndRegisterHealthChecks() {
            var env = mock(Environment.class);
            var registry = new HealthCheckRegistry();

            when(env.healthChecks()).thenReturn(registry);

            var pollerWithHealthChecks = poller.andRegisterHealthChecks(env);

            assertThat(pollerWithHealthChecks).isSameAs(poller);

            assertThat(registry.getNames()).contains(
                    ClientPollerTimeBasedHealthCheck.nameFor(poller.getName()),
                    ClientPollerLatencyBasedHealthCheck.nameFor(poller.getName()),
                    ClientPollerMissedPollHealthCheck.nameFor(poller.getName())
            );
        }

        @Test
        void testRegisterHealthChecks_UsingFluentApiMethod_AndRegisterHealthChecks_WithHealthCheckConfig() {
            var env = mock(Environment.class);
            var registry = new HealthCheckRegistry();

            when(env.healthChecks()).thenReturn(registry);

            var healthCheckConfig = PollerHealthCheckConfig.builder().build();
            var pollerWithHealthChecks = poller.andRegisterHealthChecks(env, healthCheckConfig);

            assertThat(pollerWithHealthChecks).isSameAs(poller);

            assertThat(registry.getNames()).contains(
                    ClientPollerTimeBasedHealthCheck.nameFor(poller.getName()),
                    ClientPollerLatencyBasedHealthCheck.nameFor(poller.getName()),
                    ClientPollerMissedPollHealthCheck.nameFor(poller.getName())
            );
        }

        @Test
        void testCannotStart_UnlessExecutionIntervalIsPositive() {
            var illegalExecutionIntervalPoller = ClientPoller.builder()
                    .supplier(() -> invoker)
                    .name("Test Poller")
                    .consumer(consumer)
                    .executor(executor)
                    .build();

            assertThatThrownBy(illegalExecutionIntervalPoller::start)
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("executionInterval must be a positive number of milliseconds!");
        }

        @Test
        void testCannotStart_WithoutIntervalType() {
            assertThatThrownBy(() -> poller.start(null))
                    .isExactlyInstanceOf(IllegalStateException.class)
                    .hasMessage("delayType must be specified to start polling");
        }

        @Test
        void testStarts_WithNoDelay(SoftAssertions softly) {
            when(invoker.get()).thenReturn(response);

            startPollerAndStopOnceReceiveEnoughRequests(1);

            verify(consumer, atLeastOnce()).accept(response);

            softly.assertThat(poller.statistics().successCount()).isGreaterThanOrEqualTo(1);
            softly.assertThat(poller.statistics().failureCount()).isZero();
        }

        @Test
        void testPoll_Synchronously(SoftAssertions softly) {
            final int numberOfIntervals = 3;

            when(invoker.get()).thenReturn(response);

            var syncPoller = buildSyncPoller(clientPollerStatistics -> true);

            startPollerAndStopOnceReceiveEnoughRequests(syncPoller, numberOfIntervals);

            verify(consumer, atLeast(numberOfIntervals)).accept(response);

            softly.assertThat(syncPoller.statistics().successCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(syncPoller.statistics().skipCount()).isZero();
            softly.assertThat(syncPoller.statistics().failureCount()).isZero();
        }

        @Test
        void testPoll_Synchronously_WithFixedRate(SoftAssertions softly) {
            final int numberOfIntervals = 4;

            when(invoker.get()).thenReturn(response);

            var syncPoller = buildSyncPoller(clientPollerStatistics -> true);

            startPollerAndStopOnceReceiveEnoughRequests(syncPoller, ClientPoller.DelayType.FIXED_RATE, numberOfIntervals);

            verify(consumer, atLeast(numberOfIntervals)).accept(response);

            softly.assertThat(syncPoller.statistics().successCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(syncPoller.statistics().skipCount()).isZero();
            softly.assertThat(syncPoller.statistics().failureCount()).isZero();
        }

        @Test
        void testPoll_Async(SoftAssertions softly) {
            final int numberOfIntervals = 3;

            when(invoker.get()).thenReturn(response);

            startPollerAndStopOnceReceiveEnoughRequests(numberOfIntervals);

            verify(consumer, atLeast(numberOfIntervals)).accept(response);

            softly.assertThat(poller.statistics().successCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(poller.statistics().skipCount()).isZero();
            softly.assertThat(poller.statistics().failureCount()).isZero();
        }

        @Test
        void testPollAsync_ShouldNeverExecutePoll_WhenDecisionFunctionAlwaysReturnsFalse(SoftAssertions softly) {
            poller = buildAsyncPoller(clientPollerStatistics -> false);

            final int numberOfIntervals = 5;
            startPollerAndStopOnceEnoughSkips(numberOfIntervals);

            softly.assertThat(poller.statistics().skipCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(poller.statistics().successCount()).isZero();
            softly.assertThat(poller.statistics().failureCount()).isZero();
        }

        @Test
        void testPollSync_ShouldNeverExecutePoll_WhenDecisionFunctionReturnsFalse(SoftAssertions softly) {
            when(invoker.get()).thenReturn(response);

            final int numberOfIntervals = 6;

            var count = new AtomicInteger();

            var syncPoller = buildSyncPoller(clientPollerStatistics -> {
                count.incrementAndGet();
                return false;
            });

            pollAndAssertAllSkipped(syncPoller, numberOfIntervals, count, softly);
        }

        @Test
        void testPollSync_ShouldNeverExecutePoll_WhenDecisionFunctionThrowsException(SoftAssertions softly) {
            when(invoker.get()).thenReturn(response);

            final int numberOfIntervals = 4;

            var count = new AtomicInteger();

            var syncPoller = buildSyncPoller(clientPollerStatistics -> {
                count.incrementAndGet();
                throw new RuntimeException("oops, can't make a decision, blowing up instead!");
            });

            pollAndAssertAllSkipped(syncPoller, numberOfIntervals, count, softly);
        }

        private void pollAndAssertAllSkipped(ClientPoller syncPoller, int numberOfIntervals, AtomicInteger count, SoftAssertions softly) {
            startPollerAndStopOnceEnoughSkips(syncPoller, numberOfIntervals);

            softly.assertThat(count.get()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(syncPoller.statistics().successCount()).isZero();
            softly.assertThat(syncPoller.statistics().skipCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(syncPoller.statistics().failureCount()).isZero();
        }

        /**
         * @implNote To verify that we don't let any exceptions escape in poll(), and because we have so much exception
         * handling in ClientPoller already, we had to pick something in executePoll() that we would not expect to ever
         * fail. So, we mock (actually, spy) it such that it actually does throw an exception. This test therefore verifies
         * that the poller continues to poll despite the exceptions that are being thrown after we've successfully handled
         * a poll response. An edge case to be sure, but we cannot allow exceptions to escape and cause the poller's
         * ScheduledExecutorService to terminate.
         */
        @Test
        void testPollSync_WhenExceptionOccursDuringPoll_WillIgnoreExceptions() {
            when(invoker.get()).thenReturn(response);

            ClientPollerStatistics badStats = spy(new DefaultClientPollerStatistics());
            doThrow(new RuntimeException("yes, this is a bit of white-box testing"))
                    .when(badStats)
                    .addPollLatencyMeasurement(anyLong());

            var syncPoller = ClientPoller.builder()
                    .statistics(badStats)
                    .name("Test Poller")
                    .supplier(() -> invoker)
                    .consumerType(ConsumerType.SYNC)
                    .consumer(consumer)
                    .executor(executor)
                    .initialExecutionDelay(Duration.ZERO)
                    .executionInterval(intervalInMillis)
                    .build();

            startPollerAndStopOnceReceiveEnoughRequests(syncPoller, 5);

            assertThat(syncPoller.statistics().totalCount()).isGreaterThanOrEqualTo(5);
        }

        @Test
        void testPoll_ShouldOnlyExecutePoll_WhenTotalCountIsZero(SoftAssertions softly) {
            poller = buildAsyncPoller(clientPollerStatistics -> clientPollerStatistics.totalCount() == 0);

            when(invoker.get()).thenReturn(response);

            final int minimumNumberOfSkips = 3;
            startPollerAndStopOnceEnoughSkips(minimumNumberOfSkips);

            softly.assertThat(poller.statistics().skipCount()).isGreaterThanOrEqualTo(minimumNumberOfSkips);
            softly.assertThat(poller.statistics().successCount()).isPositive();
            softly.assertThat(poller.statistics().failureCount()).isZero();
        }

        @Test
        void testPoll_WhenBadRequest(SoftAssertions softly) {
            when(invoker.get()).thenThrow(new ProcessingException("oops"));

            startPollerAndStopOnceReceiveEnoughRequests(1);

            verify(consumer, times(0)).accept(any());
            softly.assertThat(poller.statistics().totalCount()).isGreaterThanOrEqualTo(1);
            softly.assertThat(poller.statistics().successCount()).isZero();
            softly.assertThat(poller.statistics().failureCount()).isGreaterThanOrEqualTo(1);
        }

        @Test
        void testPoll_WhenResponseConsumerThrowsException(SoftAssertions softly) {
            when(invoker.get()).thenReturn(response);

            doThrow(new RuntimeException("oops")).when(consumer).accept(any());

            startPollerAndStopOnceReceiveEnoughRequests(1);

            verify(consumer, atLeast(1)).accept(response);
            softly.assertThat(poller.statistics().totalCount()).isGreaterThanOrEqualTo(1);
            softly.assertThat(poller.statistics().successCount()).isZero();
            softly.assertThat(poller.statistics().failureCount()).isGreaterThanOrEqualTo(1);
        }

        @Test
        void testPollingStatistics_WithUserSuppliedClientPollerStatistics(SoftAssertions softly) {
            final int numberOfIntervals = 3;

            when(invoker.get()).thenReturn(response);

            ClientPollerStatistics stats = new DefaultClientPollerStatistics();

            var customPoller = ClientPoller.builder()
                    .supplier(() -> invoker)
                    .name("Test Poller")
                    .consumer(consumer)
                    .executor(new ScheduledThreadPoolExecutor(5))
                    .initialExecutionDelay(Duration.ZERO)
                    .executionInterval(intervalInMillis)
                    .statistics(stats)
                    .build();

            startPollerAndStopOnceReceiveEnoughRequests(customPoller, numberOfIntervals);

            verify(consumer, atLeast(numberOfIntervals)).accept(response);
            softly.assertThat(customPoller.statistics()).isSameAs(stats);
            softly.assertThat(customPoller.statistics().successCount()).isGreaterThanOrEqualTo(numberOfIntervals);
            softly.assertThat(customPoller.statistics().failureCount()).isZero();
        }

        @Test
        void testSupplierTimeout() {
            var slowPoller = ClientPoller.builder()
                    .name("Test Poller")
                    .consumerType(ConsumerType.SYNC)
                    .consumer(consumer)
                    .supplier(() -> invoker)
                    .executor(executor)
                    .initialExecutionDelay(Duration.ZERO)
                    .executionInterval(intervalInMillis)
                    .supplierTimeout(50L)
                    .supplierTimeoutUnit(TimeUnit.MILLISECONDS)
                    .build();

            try {
                when(invoker.get()).thenAnswer(answersWithDelay(500L, invocationOnMock -> response));

                var startTime = System.currentTimeMillis();
                startPollerAndStopOnceConditionIsMet(slowPoller, () -> slowPoller.statistics().failureCount() > 0);

                assertThat(System.currentTimeMillis() - startTime).isLessThan(500);
            } finally {
                slowPoller.stop();
            }
        }

        private void startPollerAndStopOnceReceiveEnoughRequests(int minimumNumberOfRequests) {
            startPollerAndStopOnceReceiveEnoughRequests(poller, minimumNumberOfRequests);
        }

        private void startPollerAndStopOnceReceiveEnoughRequests(ClientPoller thePoller, int minimumNumberOfRequests) {
            startPollerAndStopOnceReceiveEnoughRequests(thePoller, ClientPoller.DelayType.FIXED_DELAY, minimumNumberOfRequests);
        }

        private void startPollerAndStopOnceReceiveEnoughRequests(ClientPoller thePoller, ClientPoller.DelayType delayType, int minimumNumberOfRequests) {
            var stats = thePoller.statistics();
            Callable<Boolean> condition = () -> stats.successCount() + stats.failureCount() >= minimumNumberOfRequests;
            startPollerAndStopOnceConditionIsMet(thePoller, delayType, condition);
        }

        private void startPollerAndStopOnceEnoughSkips(int minimumSkips) {
            startPollerAndStopOnceEnoughSkips(poller, minimumSkips);
        }

        private void startPollerAndStopOnceEnoughSkips(ClientPoller thePoller, int minimumSkips) {
            Callable<Boolean> condition = () -> thePoller.statistics().skipCount() >= minimumSkips;
            startPollerAndStopOnceConditionIsMet(thePoller, condition);
        }

        private void startPollerAndStopOnceConditionIsMet(ClientPoller thePoller, Callable<Boolean> condition) {
            startPollerAndStopOnceConditionIsMet(thePoller, ClientPoller.DelayType.FIXED_DELAY, condition);
        }

        private void startPollerAndStopOnceConditionIsMet(ClientPoller thePoller, ClientPoller.DelayType delayType, Callable<Boolean> condition) {
            thePoller.start(delayType);

            try {
                await().atMost(FIVE_SECONDS).until(condition);
            } catch (Exception e) {
                fail();
            } finally {
                thePoller.stop();
            }
        }
    }
}
