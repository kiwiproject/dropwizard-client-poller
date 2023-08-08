package org.kiwiproject.dropwizard.poller;

import jakarta.ws.rs.client.SyncInvoker;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Delegate;

/**
 * A wrapper for a {@link SyncInvoker} in order for the poller to have access to the original URI for logging and debugging
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
