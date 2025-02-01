package org.kiwiproject.dropwizard.poller.config;

import static org.kiwiproject.dropwizard.poller.health.ClientPollerLatencyBasedHealthCheck.DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck.DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT;
import static org.kiwiproject.dropwizard.poller.health.ClientPollerTimeBasedHealthCheck.DEFAULT_TIME_WINDOW_MINUTES;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertPropertyViolations;

import io.dropwizard.core.Configuration;
import io.dropwizard.util.Duration;
import lombok.Getter;
import lombok.Setter;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.kiwiproject.test.dropwizard.configuration.DropwizardConfigurations;
import org.kiwiproject.validation.Required;
import org.opentest4j.AssertionFailedError;

import java.nio.file.Paths;

@DisplayName("PollerHealthCheckConfig")
@ExtendWith(SoftAssertionsExtension.class)
class PollerHealthCheckConfigTest {

    private PollerHealthCheckConfig config;

    @Nested
    class Builder {

        @Test
        void shouldSetDefaultValues(SoftAssertions softly) {
            config = PollerHealthCheckConfig.builder().build();
            assertDefaultValues(softly);
        }

        @Test
        void shouldSetDefaultValues_WhenExplicitlyGivenAllNulls(SoftAssertions softly) {
            config = PollerHealthCheckConfig.builder()
                    .timeWindow(null)
                    .failedPollsThresholdPercent(null)
                    .averageLatencyWarningThreshold(null)
                    .missingPollMultiplier(null)
                    .build();

            assertDefaultValues(softly);
        }

        private void assertDefaultValues(SoftAssertions softly) {
            softly.assertThat(config.getTimeWindow().toMinutes()).isEqualTo(15);
            softly.assertThat(config.getFailedPollsThresholdPercent()).isEqualTo(2);
            softly.assertThat(config.getAverageLatencyWarningThreshold().toMilliseconds()).isEqualTo(DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS);
            softly.assertThat(config.getMissingPollMultiplier()).isEqualTo(10);
        }
    }

    @Nested
    class DeserializedFromYaml {

        @Test
        void shouldDeserializeFullConfig(SoftAssertions softly) {
            config = deserializeAndExtractConfig("full-config.yml");

            softly.assertThat(config.getTimeWindow().toMinutes()).isEqualTo(10);
            softly.assertThat(config.getFailedPollsThresholdPercent()).isEqualTo(3);
            softly.assertThat(config.getAverageLatencyWarningThreshold().toSeconds()).isEqualTo(5);
            softly.assertThat(config.getMissingPollMultiplier()).isEqualTo(15);
        }

        @Test
        void shouldRespectDefaultValues(SoftAssertions softly) {
            config = deserializeAndExtractConfig("minimal-config.yml");

            softly.assertThat(config.getTimeWindow().toMinutes()).isEqualTo(DEFAULT_TIME_WINDOW_MINUTES);
            softly.assertThat(config.getFailedPollsThresholdPercent()).isEqualTo(DEFAULT_FAILED_POLLS_UNHEALTHY_THRESHOLD_PERCENT);
            softly.assertThat(config.getAverageLatencyWarningThreshold().toMilliseconds()).isEqualTo(DEFAULT_AVG_LATENCY_WARNING_THRESHOLD_MILLIS);
            softly.assertThat(config.getMissingPollMultiplier()).isEqualTo(12); // not the default, but need at least one property
        }

        private PollerHealthCheckConfig deserializeAndExtractConfig(String configFileName) {
            var path = Paths.get("PollerHealthCheckConfigTest", configFileName);

            try {
                var sampleConfig = DropwizardConfigurations.newConfiguration(SampleConfig.class, path);

                return sampleConfig.getHealthCheckConfig();
            } catch (Exception e) {
                throw new AssertionFailedError("Error de-serializing config at path: " + path, e);
            }
        }
    }

    @Getter
    @Setter
    public static class SampleConfig extends Configuration {
        private PollerHealthCheckConfig healthCheckConfig;

        @Required
        private String dummyValue;
    }

    @Nested
    class Validation {

        @ParameterizedTest
        @CsvSource({
                "0 minutes, false",
                "1 minutes, true",
                "2 minutes, true",
                "15 minutes, true",
                "24 hours, true",
                "1441 minutes, false", // 24 hours, 1 minute
                "25 hours, false"
        })
        void shouldValidate_timeWindow(String durationString, boolean isValid) {
            config = PollerHealthCheckConfig.builder()
                    .timeWindow(Duration.parse(durationString))
                    .build();

            int numExpectedViolations = numExpectedViolations(isValid);

            assertPropertyViolations(config, "timeWindow", numExpectedViolations);
        }

        @ParameterizedTest
        @CsvSource({
                "0, false",
                "1, true",
                "50, true",
                "99, true",
                "100, false"
        })
        void shouldValidate_failedPollsThresholdPercent(int percent, boolean isValid) {
            config = PollerHealthCheckConfig.builder()
                    .failedPollsThresholdPercent(percent)
                    .build();

            int numExpectedViolations = numExpectedViolations(isValid);

            assertPropertyViolations(config, "failedPollsThresholdPercent", numExpectedViolations);
        }

        @ParameterizedTest
        @CsvSource({
                "4 milliseconds, false",
                "5 milliseconds, true",
                "3500 milliseconds, true",
                "10 minutes, true",
                "59 minutes, true",
                "60 minutes, true",
                "3600001 milliseconds, false" // 1 hour and 1 millisecond
        })
        void shouldValidate_averageLatencyWarningThreshold(String durationString, boolean isValid) {
            config = PollerHealthCheckConfig.builder()
                    .averageLatencyWarningThreshold(Duration.parse(durationString))
                    .build();

            var numExpectedViolations = numExpectedViolations(isValid);

            assertPropertyViolations(config, "averageLatencyWarningThreshold", numExpectedViolations);
        }

        @ParameterizedTest
        @CsvSource({
                "4, false",
                "5, true",
                "25, true",
                "50, true",
                "51, false"
        })
        void shouldValidate_missingPollMultiplier(int missingPollMultiplier, boolean isValid) {
            config = PollerHealthCheckConfig.builder()
                    .missingPollMultiplier(missingPollMultiplier)
                    .build();

            var numExpectedViolations = numExpectedViolations(isValid);

            assertPropertyViolations(config, "missingPollMultiplier", numExpectedViolations);
        }

        private int numExpectedViolations(boolean isValid) {
            return isValid ? 0 : 1;
        }
    }
}
