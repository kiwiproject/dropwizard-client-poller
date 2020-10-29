package org.kiwiproject.dropwizard.poller;

/**
 * Defines how to consume responses in the {@link ClientPoller}, either synchronously or asynchronously.
 */
public enum ConsumerType {
    ASYNC, SYNC;

    public boolean isAsync() {
        return this == ASYNC;
    }

    public boolean isSync() {
        return this == SYNC;
    }
}
