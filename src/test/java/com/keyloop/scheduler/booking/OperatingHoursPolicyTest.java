package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class OperatingHoursPolicyTest {
    private final Dealership london = dealership("08:00", "18:00");

    @ParameterizedTest
    @CsvSource({
            "2030-01-07T08:00:00Z,2030-01-07T09:00:00Z",
            "2030-01-07T17:00:00Z,2030-01-07T18:00:00Z",
            "2030-07-07T07:00:00Z,2030-07-07T08:00:00Z",
            "2030-07-07T16:00:00Z,2030-07-07T17:00:00Z"
    })
    void acceptsExactOpeningAndClosingInWinterAndSummer(String start, String end) {
        assertThatCode(() -> OperatingHoursPolicy.validate(london, slot(start, end))).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @CsvSource({
            "2030-01-07T02:00:00Z,2030-01-07T03:00:00Z",
            "2030-01-07T07:59:00Z,2030-01-07T08:59:00Z",
            "2030-01-07T17:30:00Z,2030-01-07T18:30:00Z",
            "2030-01-07T17:00:00Z,2030-01-08T09:00:00Z",
            "2030-07-07T06:30:00Z,2030-07-07T07:30:00Z",
            "2030-07-07T16:30:00Z,2030-07-07T17:30:00Z"
    })
    void rejectsAnyPortionOutsideTheLocalOpeningWindow(String start, String end) {
        assertOutside(london, slot(start, end));
    }

    @ParameterizedTest
    @CsvSource({
            "2030-03-31T02:00:00Z,2030-03-31T03:00:00Z",
            "2030-10-27T02:00:00Z,2030-10-27T03:00:00Z"
    })
    void rejectsNonexistentOrAmbiguousOpeningBoundary(String start, String end) {
        assertOutside(dealership("01:30", "04:00"), slot(start, end));
    }

    @ParameterizedTest
    @CsvSource({
            "2030-03-31T00:30:00Z,2030-03-31T02:30:00Z",
            "2030-10-27T00:30:00Z,2030-10-27T02:30:00Z"
    })
    void permitsElapsedDurationAcrossDstTransitionWhenBoundariesAreUnambiguous(String start, String end) {
        assertThatCode(() -> OperatingHoursPolicy.validate(dealership("00:00", "04:00"), slot(start, end)))
                .doesNotThrowAnyException();
    }

    private void assertOutside(Dealership dealership, TimeSlot slot) {
        assertThatThrownBy(() -> OperatingHoursPolicy.validate(dealership, slot))
                .isInstanceOfSatisfying(BookingRejectedException.class,
                        error -> assertThat(error.reason()).isEqualTo(RejectionReason.OUTSIDE_OPERATING_HOURS));
    }

    private TimeSlot slot(String start, String end) {
        return new TimeSlot(Instant.parse(start), Instant.parse(end));
    }

    private Dealership dealership(String opens, String closes) {
        return new Dealership(UUID.fromString("11111111-1111-1111-1111-111111111111"), "London",
                ZoneId.of("Europe/London"), LocalTime.parse(opens), LocalTime.parse(closes));
    }
}
