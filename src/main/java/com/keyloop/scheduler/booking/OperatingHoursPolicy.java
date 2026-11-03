package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.catalog.Dealership;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

public final class OperatingHoursPolicy {
    private OperatingHoursPolicy() { }

    public static void validate(Dealership dealership, TimeSlot slot) {
        var localDate = slot.start().atZone(dealership.timezone()).toLocalDate();
        Instant opens = boundary(localDate.atTime(dealership.openTime()), dealership.timezone());
        Instant closes = boundary(localDate.atTime(dealership.closeTime()), dealership.timezone());
        if (slot.start().isBefore(opens) || slot.end().isAfter(closes)) {
            throw outsideHours();
        }
    }

    private static Instant boundary(LocalDateTime localTime, ZoneId zone) {
        List<ZoneOffset> offsets = zone.getRules().getValidOffsets(localTime);
        if (offsets.size() != 1) {
            throw outsideHours();
        }
        return localTime.toInstant(offsets.getFirst());
    }

    private static BookingRejectedException outsideHours() {
        return new BookingRejectedException(RejectionReason.OUTSIDE_OPERATING_HOURS,
                "The full appointment must fit within unambiguous dealership opening hours");
    }
}
