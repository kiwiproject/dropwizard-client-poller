package org.kiwiproject.dropwizard.poller;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiStrings.format;
import static org.kiwiproject.concurrent.Async.doAsync;
import static org.kiwiproject.concurrent.Async.withMaxTimeout;
import static org.kiwiproject.jaxrs.KiwiResponses.closeQuietly;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.setup.Environment;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.kiwiproject.dropwizard.poller.config.PollerHealthCheckConfig;
import org.kiwiproject.dropwizard.poller.health.ClientPollerHealthChecks;
import org.kiwiproject.dropwizard.poller.metrics.ClientPollerStatistics;
import org.kiwiproject.dropwizard.poller.metrics.DefaultClientPollerStatistics;

import javax.ws.rs.client.SyncInvoker;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Polls a specified HTTP(S) endpoint either synchronously or asynchronously using a JAX-RS {@link SyncInvoker}.
 */
@Builder
@Slf4j
public class ClientPoller {

    /**
     * The default timeout value when polling synchronously
     */
    public static final long DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT = 5;

    /**
     * The default timeout unit when polling synchronously
     */
    public static final TimeUnit DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT_UNIT = TimeUnit.MINUTES;

    /**
     * The default timeout value for the {@link #supplier}
     */
    public static final long DEFAULT_SUPPLIER_TIMEOUT = 90;

    /**
     * The default timeout unit for the {@link #supplier}
     */
    public static final TimeUnit DEFAULT_SUPPLIER_TIMEOUT_UNIT = TimeUnit.SECONDS;

    /**
     * The default duration to wait before the first execution
     */
    public static final Duration DEFAULT_INITIAL_EXECUTION_DELAY = Duration.ofSeconds(5);

    /**
     * Whether to poll using fixed rate or with fixed delay, as defined by {@link ScheduledExecutorService}.
     *
     * @implNote This enum should only have TWO values corresponding to the two ways of scheduling on a
     * {@link ScheduledExecutorService}. There is no way to enforce that, however.
     * @see ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)
     * @see ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)
     */
    public enum DelayType {
        FIXED_RATE, FIXED_DELAY
    }

    /**
     * The URI the poller is polling. This will be set if the {@link SyncInvoker} provided is an instance of
     * {@link PollerSyncInvokerWrapper}
     * <p>
     * NOTE: only used for logging purposes. It will be overwritten so we should not ever manually set it.
     */
    private URI uri;

    /**
     * A descriptive name for this poller. This will be used in logging and health checks to indicate which poller this is
     */
    @Getter
    @Builder.Default
    private final String name = "Poller_" + System.currentTimeMillis();

    /**
     * Supplies the invoker used to make the repeating REST call. <em>Required</em>
     */
    private final Supplier<SyncInvoker> supplier;

    /**
     * The consumer that acts on the {@code Optional<Response>} from {@link SyncInvoker#get()}. <em>Required.</em>
     */
    private final Consumer<Response> consumer;

    /**
     * How the consumer should handle responses, sync or async. <em>Default is async</em,
     */
    @Builder.Default
    private final ConsumerType consumerType = ConsumerType.ASYNC;

    /**
     * The value after which a synchronous response consumer will time out.
     */
    @Getter(AccessLevel.PACKAGE)
    @Builder.Default
    private final Long syncConsumerTimeout = DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT;

    /**
     * The unit of the synchronous response consumer time out value.
     */
    @Getter(AccessLevel.PACKAGE)
    @Builder.Default
    private final TimeUnit syncConsumerTimeoutUnit = DEFAULT_SYNC_RESPONSE_CONSUMER_TIMEOUT_UNIT;

    /**
     * The value after which the {@link #supplier} will time out, e.g. if a poll is taking a very long time.
     */
    @Getter(AccessLevel.PACKAGE)
    @Builder.Default
    private final Long supplierTimeout = DEFAULT_SUPPLIER_TIMEOUT;

    /**
     * The unit of the {@link #supplierTimeout}.
     */
    @Getter(AccessLevel.PACKAGE)
    @Builder.Default
    private final TimeUnit supplierTimeoutUnit = DEFAULT_SUPPLIER_TIMEOUT_UNIT;

    /**
     * The delay before polling will begin.
     */
    @Builder.Default
    private final Duration initialExecutionDelay = DEFAULT_INITIAL_EXECUTION_DELAY;

    /**
     * The decision function that determines whether a poll should actually occur when the executor executes.
     * <p>
     * If not provided, a default decision function is used that always returns true.
     */
    @Builder.Default
    private final Function<ClientPollerStatistics, Boolean> decisionFunction = clientPollerStatistics -> true;

    /**
     * Interval by which the poller polls (in milliseconds). <em>Required as a positive value.</em>
     */
    @Getter
    private final long executionInterval;

    /**
     * The executor the poller should use to scheduled a fixed-rate or fixed-delay poll. <em>Required.</em>
     */
    private final ScheduledExecutorService executor;

    /**
     * The default consumer executor service (using the default factory). Available to override if necessary.
     */
    private ExecutorService consumerExecutor;

    /**
     * The {@link ClientPollerStatistics} instance to use to collect statistics. <em>If not specified an
     * instance of {@link DefaultClientPollerStatistics} will be initialized and used.</em>
     *
     * @implNote Each poller should have its own statistics instance (as it does not make sense to share statistics
     * between multiple pollers and could lead to misleading or erroneous results). Also
     */
    @Builder.Default
    private final ClientPollerStatistics statistics = ClientPollerStatistics.newClientPollerStatisticsOfDefaultType();

    /**
     * Used to track whether this poller is polling or not.
     * <p>
     * Lifecycle: build() -> start() -> stop() will result in sequence of values false -> true -> false.
     */
    private final AtomicBoolean polling = new AtomicBoolean();

    /**
     * Registers client poller health checks on this poller.
     *
     * @param environment the Dropwizard environment
     * @return this poller
     * @see ClientPollerHealthChecks#registerPollerHealthChecks(ClientPoller, Environment)
     */
    public ClientPoller registerHealthChecks(Environment environment) {
        return registerHealthChecks(environment, PollerHealthCheckConfig.builder().build());
    }

    /**
     * Registers client poller health checks on this poller using the given {@link PollerHealthCheckConfig}.
     *
     * @param environment       the Dropwizard environment
     * @param healthCheckConfig the configuration for health checks
     * @return this poller
     * @see ClientPollerHealthChecks#registerPollerHealthChecks(ClientPoller, Environment)
     */
    public ClientPoller registerHealthChecks(Environment environment, PollerHealthCheckConfig healthCheckConfig) {
        ClientPollerHealthChecks.registerPollerHealthChecks(this, environment, healthCheckConfig);
        return this;
    }

    /**
     * Named specifically to be used as part of the fluent builder API, e.g.
     * {@code poller = ClientPoller.builder()...build().andRegisterHealthChecks(env);}
     *
     * @param environment the Dropwizard environment
     * @return this poller
     */
    public ClientPoller andRegisterHealthChecks(Environment environment) {
        return registerHealthChecks(environment);
    }

    /**
     * Named specifically to be used as part of the fluent builder API, e.g.
     * {@code poller = ClientPoller.builder()...build().andRegisterHealthChecks(env, healthConfig);}
     *
     * @param environment       the Dropwizard environment
     * @param healthCheckConfig the configuration for health checks
     * @return this poller
     */
    public ClientPoller andRegisterHealthChecks(Environment environment, PollerHealthCheckConfig healthCheckConfig) {
        return registerHealthChecks(environment, healthCheckConfig);
    }

    /**
     * Starts the poller with default delay type of FIXED_DELAY
     */
    public void start() {
        start(DelayType.FIXED_DELAY);
    }

    public void start(DelayType intervalType) {
        initializeExecutorIfNull();
        validateInternalState();
        checkState(nonNull(intervalType), "intervalType must be specified to start polling");
        scheduleExecution(intervalType);
        polling.set(true);
    }

    private void initializeExecutorIfNull() {
        if (isNull(consumerExecutor)) {
            consumerExecutor = buildDefaultConsumerExecutor(statistics);
        }
    }

    private void scheduleExecution(DelayType intervalType) {
        if (intervalType == DelayType.FIXED_RATE) {
            executor.scheduleAtFixedRate(this::poll, initialExecutionDelay.toMillis(), executionInterval, TimeUnit.MILLISECONDS);
        } else {
            executor.scheduleWithFixedDelay(this::poll, initialExecutionDelay.toMillis(), executionInterval, TimeUnit.MILLISECONDS);
        }
        LOG.debug("{} - Execution scheduled: with interval of {}ms [{}]", name, executionInterval, intervalType);
    }

    /**
     * @implNote We could replace this validation with Lombok's {@code @NonNull} annotation. However we would
     * then get {@link NullPointerException} when calling the build method on the builder, instead of
     * the {@link IllegalStateException} that is thrown here. It seems throwing an {@link IllegalStateException}
     * is more appropriate than NPE in this case, or at least more clear.
     */
    private void validateInternalState() {
        checkState(nonNull(supplier), "supplier cannot be null");
        checkState(nonNull(consumerType), "consumerType cannot be null");

        if (consumerType.isSync()) {
            checkState(nonNull(syncConsumerTimeout), "syncConsumerTimeout cannot be null for sync poller");
            checkState(syncConsumerTimeout > 0, "syncConsumerTimeout must be greater than zero");
            checkState(nonNull(syncConsumerTimeoutUnit), "syncConsumerTimeoutUnit cannot be null for sync poller");
        }

        checkState(nonNull(supplierTimeout), "supplierTimeout cannot be null");
        checkState(supplierTimeout > 0, "supplierTimeout must be greater than zero");
        checkState(nonNull(supplierTimeoutUnit), "supplierTimeoutUnit cannot be null");
        checkState(nonNull(initialExecutionDelay), "initialExecutionDelay cannot be null");
        checkState(nonNull(consumer), "consumer cannot be null");
        checkState(nonNull(decisionFunction), "decisionFunction cannot be null");
        checkState(executionInterval > 0, "executionInterval must be a positive number of milliseconds!");
        checkState(nonNull(executor), "executor cannot be null");
        checkState(nonNull(consumerExecutor), "consumerExecutor cannot be null");
    }

    private void poll() {
        try {
            if (shouldNotExecutePoll()) {
                LOG.trace("{} - Skipping poll; decision function returned false", name);
                statistics().incrementSkipCount();
                return;
            }
            executePoll();
        } catch (Exception ex) {
            LOG.error("{} - Encountered unexpected error during polling: {}", name, ex.getMessage(), ex);
        }
    }

    private boolean shouldNotExecutePoll() {
        return !shouldExecutePoll();
    }

    private boolean shouldExecutePoll() {
        try {
            return decisionFunction.apply(statistics());
        } catch (Exception ex) {
            LOG.warn("{} - Encountered an exception while making polling decision, returning FALSE - {}", name, ex.getMessage());
            LOG.debug("{} - Decision function exception details", name, ex);
            return false;
        }
    }

    private void executePoll() {
        LOG.trace("{} - Poller executing", name);
        long start = System.currentTimeMillis();
        Response response = executePollRequest();
        handleResponse(response);
        long elapsed = System.currentTimeMillis() - start;
        statistics().addPollLatencyMeasurement(elapsed);
        LOG.trace("{} - Poll time: {} millis", name, elapsed);
    }

    @SuppressWarnings({"java:S1612", "Convert2MethodRef"})
    private Response executePollRequest() {
        statistics().incrementCount();
        try {
            SyncInvoker invoker = supplier.get();
            updateUri(invoker);

            Supplier<Response> asyncSupplier = () -> invoker.get();
            var asyncFuture = doAsync(asyncSupplier);
            var requestFuture = withMaxTimeout(asyncFuture, supplierTimeout, supplierTimeoutUnit);

            return requestFuture.get();
        } catch (final Exception e) {
            statistics().incrementFailureCount(e);
            LOG.error("{} - Poller HTTP GET request failed. URI: {}", name, uriOrDefault());
            LOG.info("{} - Poller failure:", name, e);
            return null;
        } finally {
            uri = null;
        }
    }

    private void updateUri(SyncInvoker invoker) {
        if (invoker instanceof PollerSyncInvokerWrapper) {
            String currentUri = ((PollerSyncInvokerWrapper) invoker).getUri();
            uri = UriBuilder.fromPath(currentUri).build();
        }
    }

    private String uriOrDefault() {
        if (isNull(uri)) {
            return "UNKNOWN URI... USE PollerSyncInvokerWrapper";
        }
        return uri.toString();
    }

    private void handleResponse(Response response) {
        if (nonNull(response)) {
            handleNonNullResponse(response);
        } else {
            LOG.debug("{} - Poller received null response", name);
        }
    }

    private void handleNonNullResponse(final Response response) {
        requireNonNull(response);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> consume(response), consumerExecutor);
        if (consumerType.isSync()) {
            waitForCompletion(future);
        }
    }

    private void consume(Response response) {
        int status = response.getStatus();
        LOG.trace("{} - Consuming {} response from {}", name, status, uriOrDefault());
        try {
            consumer.accept(response);
            statistics().incrementSuccessCount();
        } catch (final Exception e) {
            statistics().incrementFailureCount(e);
            LOG.error("{} - Poller error handling {} response from {}:", name, status, uriOrDefault(), e);
        } finally {
            LOG.trace("{} - Ensuring {} response is closed from {}", name, status, uriOrDefault());
            closeQuietly(response);
        }
    }

    @VisibleForTesting
    void waitForCompletion(CompletableFuture<Void> future) {
        LOG.trace("{} - Waiting up to {} {} for completion", name, syncConsumerTimeout, syncConsumerTimeoutUnit);
        try {
            future.get(syncConsumerTimeout, syncConsumerTimeoutUnit);
        } catch (InterruptedException e) {
            LOG.error("{} - Interrupted consuming response synchronously", name, e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.error("{} - ExecutionException consuming response synchronously", name, e);
        } catch (TimeoutException e) {
            LOG.error("{} - Timed-out after {} {} consuming response synchronously", name, syncConsumerTimeout, syncConsumerTimeoutUnit);
        }
    }

    /**
     * Returns the {@link ClientPollerStatistics} instance that is collecting stats for this poller.
     *
     * @return the statistics used for collecting stats for this poller
     */
    public ClientPollerStatistics statistics() {
        return statistics;
    }

    /**
     * Return {@code true} if this poller is asynchronous, otherwise {@code false}.
     *
     * @return {@code true} if this poller is asynchronous, otherwise {@code false}
     */
    public boolean isAsync() {
        return consumerType.isAsync();
    }

    /**
     * Return {@code true} if this poller is currently polling, or {@code false} if it has not been started yet
     * or if it has been stopped.
     *
     * @return true if the poller is currently polling; false otherwise
     */
    public boolean isPolling() {
        return polling.get();
    }

    /**
     * Stops the poller.
     */
    public void stop() {
        shutdownQuietly("executor", executor);
        shutdownQuietly("consumerExecutor", consumerExecutor);
        polling.set(false);
    }

    // ARCH-REV Move to Kiwi?
    @VisibleForTesting
    static void shutdownQuietly(String name, ExecutorService executor) {
        if (isNull(executor)) {
            LOG.info("Ignoring shutdown request for '{}' executor; it is null", name);
            return;
        }

        final var timeout = 5;
        final var timeUnit = TimeUnit.SECONDS;
        try {
            executor.shutdown();
            var terminatedBeforeTimeout = executor.awaitTermination(timeout, timeUnit);
            logAwaitTerminationResult(name, terminatedBeforeTimeout, timeout, timeUnit);
        } catch (Exception e) {
            LOG.warn("Unable to shut down '{}'. executor", name, e);
        }
    }

    private static void logAwaitTerminationResult(String name,
                                                  boolean terminatedBeforeTimeout,
                                                  @SuppressWarnings("SameParameterValue") int timeout,
                                                  TimeUnit timeUnit) {
        if (!terminatedBeforeTimeout) {
            LOG.warn("Executor '{}' did not shut down within {} {}", name, timeout, timeUnit);
        }
    }

    private static ExecutorService buildDefaultConsumerExecutor(ClientPollerStatistics stats) {
        ThreadFactory factory = buildDefaultConsumerThreadFactory(stats);
        return Executors.newCachedThreadPool(factory);
    }

    private static ThreadFactory buildDefaultConsumerThreadFactory(ClientPollerStatistics stats) {
        checkArgumentNotNull(stats, "stats cannot be null");
        return new BasicThreadFactory.Builder()
                .namingPattern("DefaultConsumerThread-%d")
                .daemon(false)
                .priority(Thread.NORM_PRIORITY)
                .uncaughtExceptionHandler(newUncaughtExceptionHandler(stats))
                .build();
    }

    @VisibleForTesting
    static Thread.UncaughtExceptionHandler newUncaughtExceptionHandler(ClientPollerStatistics stats) {
        return (thread, error) -> {
            String msg = format("Uncaught exception on thread {}", thread);
            LOG.error(msg, error);
            stats.incrementFailureCount(error);
        };
    }
}
