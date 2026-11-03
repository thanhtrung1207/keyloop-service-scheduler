package com.keyloop.scheduler.booking;

import java.time.Instant;
import java.util.Objects;

public record TimeSlot(Instant start, Instant end) {
    public TimeSlot {
        Objects.requireNonNull(start, "start");
        Objects.requireNonNull(end, "end");
        if (!end.isAfter(start)) {
            throw new IllegalArgumentException("end must be after start");
        }
    }

    public boolean overlaps(TimeSlot other) {
        Objects.requireNonNull(other, "other");
        return start.isBefore(other.end) && end.isAfter(other.start);
    }
}
