package com.keyloop.scheduler.booking;

import java.time.Instant;
import java.util.UUID;

public record Appointment(UUID id, UUID dealershipId, String dealershipName,
                          UUID serviceTypeId, String serviceTypeCode, String serviceTypeName,
                          UUID serviceBayId, String bayCode, UUID technicianId, String technicianName,
                          String customerId, String vehicleVin, Instant startTime, Instant endTime,
                          String status, Instant createdAt) { }
