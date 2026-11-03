package com.keyloop.scheduler.booking;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class TimeSlotTest {
    private static final Instant NINE = Instant.parse("2030-01-07T09:00:00Z");
    private static final Instant TEN = Instant.parse("2030-01-07T10:00:00Z");
    private static final Instant ELEVEN = Instant.parse("2030-01-07T11:00:00Z");

    @Test
    void overlappingIntervalsAreDetected() {
        assertThat(new TimeSlot(NINE, TEN).overlaps(new TimeSlot(Instant.parse("2030-01-07T09:30:00Z"), ELEVEN)))
                .isTrue();
    }

    @Test
    void touchingEndpointsAreAdjacentRatherThanOverlapping() {
        assertThat(new TimeSlot(NINE, TEN).overlaps(new TimeSlot(TEN, ELEVEN))).isFalse();
    }

    @Test
    void containedIntervalOverlaps() {
        assertThat(new TimeSlot(NINE, ELEVEN).overlaps(new TimeSlot(TEN, Instant.parse("2030-01-07T10:30:00Z"))))
                .isTrue();
    }

    @Test
    void rejectsNullEndpoints() {
        assertThatNullPointerException().isThrownBy(() -> new TimeSlot(null, TEN));
        assertThatNullPointerException().isThrownBy(() -> new TimeSlot(NINE, null));
    }

    @Test
    void rejectsEmptyOrNegativeIntervals() {
        assertThatIllegalArgumentException().isThrownBy(() -> new TimeSlot(NINE, NINE));
        assertThatIllegalArgumentException().isThrownBy(() -> new TimeSlot(TEN, NINE));
    }
}
