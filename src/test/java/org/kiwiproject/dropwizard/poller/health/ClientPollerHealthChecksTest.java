package org.kiwiproject.dropwizard.poller.health;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.base.KiwiStrings.f;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.poller.metrics.DefaultClientPollerStatistics;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@DisplayName("ClientPollerHealthChecks")
class ClientPollerHealthChecksTest {

    @Nested
    class NameFor {

        @Test
        void shouldSetNameToUnspecifiedName_WhenNullUriObjectIsGiven() {
            var name = ClientPollerHealthChecks.nameFor("typeA", (URI) null);
            assertUnspecifiedUriName(name, "typeA");
        }

        @Test
        void shouldSetNameToUnspecifiedName_WhenNullUriStringIsGiven() {
            var name = ClientPollerHealthChecks.nameFor("typeB", (String) null);
            assertUnspecifiedUriName(name, "typeB");
        }

        private void assertUnspecifiedUriName(String name, String healthCheckType) {
            var partialCurrentTimeMillis = String.valueOf(System.currentTimeMillis()).substring(0, 9);

            assertThat(name)
                    .startsWith("client-poller-" + healthCheckType + ":[unspecified-URI")
                    .contains(partialCurrentTimeMillis)
                    .endsWith("]");
        }

        @Test
        void shouldBuildName_WhenValidUriIsGiven() {
            var uri = URI.create("http://localhost:8765/some/endpoint");
            assertThat(ClientPollerHealthChecks.nameFor("typeX", uri)).isEqualTo("client-poller-typeX:" + uri);
        }

        @Test
        void shouldBuildName_WhenValidUriStringIsGiven() {
            var uri = "http://localhost:8765/some/endpoint";
            assertThat(ClientPollerHealthChecks.nameFor("typeY", uri)).isEqualTo("client-poller-typeY:" + uri);
        }
    }

    @Nested
    class Unhealthy {

        @Test
        void shouldAddDetails() {
            var stats = new DefaultClientPollerStatistics();

            IntStream.range(0, 100)
                    .forEach(value -> stats.incrementFailureCount(new Exception(f("error{}", value))));

            var failedResult = ClientPollerHealthChecks.unhealthy(stats, "testMessage %s %d", "foo", 42);

            List<Map<String, Object>> failureDetails = stats.recentFailureDetails().collect(toList());

            assertThat(failedResult.isHealthy()).isFalse();
            assertThat(failedResult.getMessage()).isEqualTo("testMessage foo 42");
            assertThat(failedResult.getDetails()).containsEntry(ClientPollerHealthChecks.FAILURE_DETAILS_KEY, failureDetails);
            assertThat(failedResult.getDetails()).containsEntry("severity", "WARN");
        }
    }
}
