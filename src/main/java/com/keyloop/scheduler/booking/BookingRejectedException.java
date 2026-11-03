package com.keyloop.scheduler.booking;

public class BookingRejectedException extends RuntimeException {
    private final RejectionReason reason;

    public BookingRejectedException(RejectionReason reason) {
        this(reason, reason.name());
    }

    public BookingRejectedException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
