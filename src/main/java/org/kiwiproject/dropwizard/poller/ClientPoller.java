package org.kiwiproject.dropwizard.poller;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.client.SyncInvoker;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Utilizes the core client to poll a specified endpoint
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
     * <p>
     * @see ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)
     * @see ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)
     * @implNote This enum should only have TWO values corresponding to the two ways of scheduling on a
     * {@link ScheduledExecutorService}. There is no way to enforce that, however.
     */
    public enum DelayType {
        FIXED_RATE, FIXED_DELAY
    }

    /**
     * The URI the poller is polling. <em>Not required, but recommended</em>
     * <p>
     * NOTE: only used for logging purposes. It will be overwritten so we should not ever manually set it.
     */
    private URI uri;

    /**
     * A descriptive name for this poller. This will be used in logging and health checks to indicate which poller this is
     */
    @Getter
    @Builder.Default
    private String name = "Poller_" + System.currentTimeMillis();

    /**
     * Supplies the invoker used to make the repeating REST call. <em>Required</em>
     */
    private final Supplier<SyncInvoker> supplier;
}
