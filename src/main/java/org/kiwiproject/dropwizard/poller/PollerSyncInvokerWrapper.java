package org.kiwiproject.dropwizard.poller;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;

import javax.ws.rs.client.SyncInvoker;

/**
 * A wrapper for a {@link SyncInvoker} in order for the poller to have access to the original uri for logging and debugging
 */
@SuppressWarnings("WeakerAccess")
@Getter
@Setter
@Builder
public class PollerSyncInvokerWrapper implements SyncInvoker {
    private String uri;

    @Delegate
    private SyncInvoker delegateSyncInvoker;
}
