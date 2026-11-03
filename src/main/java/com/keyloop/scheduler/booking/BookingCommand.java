package com.keyloop.scheduler.booking;

import java.time.Instant;
import java.util.UUID;

public record BookingCommand(UUID dealershipId, String customerId, String vehicleVin,
                             UUID serviceTypeId, Instant desiredStartTime) { }
