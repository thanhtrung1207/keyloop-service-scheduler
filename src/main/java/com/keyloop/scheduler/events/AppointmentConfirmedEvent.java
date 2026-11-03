package com.keyloop.scheduler.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.catalog.ServiceType;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record AppointmentConfirmedEvent(String specversion, UUID id, String source, String type,
                                        String subject, Instant time, String datacontenttype, Data data) {
    public static final String TYPE = "com.keyloop.ownership.appointment.confirmed.v1";

    public static AppointmentConfirmedEvent from(UUID eventId, Appointment appointment, ServiceType serviceType) {
        return new AppointmentConfirmedEvent("1.0", eventId, "/keyloop/ownership/unified-service-scheduler",
                TYPE, appointment.id().toString(), appointment.createdAt(), "application/json",
                new Data(appointment.id(), appointment.dealershipId(), appointment.dealershipName(),
                        appointment.customerId(), appointment.vehicleVin(), serviceType,
                        new ServiceBay(appointment.serviceBayId(), appointment.bayCode()),
                        new Technician(appointment.technicianId(), appointment.technicianName()),
                        appointment.startTime(), appointment.endTime(), appointment.status(),
                        appointment.createdAt(), null));
    }

    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record Data(UUID appointmentId, UUID dealershipId, String dealershipName,
                       String customerId, String vehicleVin, ServiceType serviceType,
                       ServiceBay allocatedServiceBay, Technician assignedTechnician,
                       Instant startTime, Instant endTime, String status, Instant createdAt, Instant cancelledAt) { }
    public record ServiceBay(UUID id, String bayCode) { }
    public record Technician(UUID id, String fullName) { }
}
