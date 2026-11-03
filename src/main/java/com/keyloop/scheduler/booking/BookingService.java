package com.keyloop.scheduler.booking;

import com.keyloop.scheduler.booking.BookingRepository.AvailableBay;
import com.keyloop.scheduler.booking.BookingRepository.AvailableTechnician;
import com.keyloop.scheduler.catalog.Dealership;
import com.keyloop.scheduler.catalog.ServiceType;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
public class BookingService {
    private static final Pattern VIN_PATTERN = Pattern.compile("^[A-HJ-NPR-Z0-9]{17}$");

    private final BookingRepository repository;
    private final Clock clock;

    public BookingService(BookingRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    @Transactional
    public Appointment book(BookingCommand command) {
        Dealership dealership = repository.findDealership(command.dealershipId())
                .orElseThrow(() -> rejected(RejectionReason.DEALERSHIP_NOT_FOUND, "Dealership was not found"));
        ServiceType serviceType = repository.findServiceType(command.serviceTypeId())
                .orElseThrow(() -> rejected(RejectionReason.SERVICE_TYPE_NOT_FOUND, "Service type was not found"));

        if (command.vehicleVin() == null || !VIN_PATTERN.matcher(command.vehicleVin()).matches()) {
            throw rejected(RejectionReason.INVALID_VIN, "VIN must be 17 uppercase ISO 3779 characters");
        }

        Instant start = command.desiredStartTime().truncatedTo(ChronoUnit.MICROS);
        if (start.isBefore(clock.instant())) {
            throw rejected(RejectionReason.START_TIME_IN_PAST, "Appointment start time must be in the future");
        }
        Instant end = start.plus(serviceType.durationMinutes(), ChronoUnit.MINUTES);
        TimeSlot slot = new TimeSlot(start, end);

        repository.lockDealership(dealership.id());

        if (repository.hasVinOverlap(command.vehicleVin(), slot)) {
            throw rejected(RejectionReason.VEHICLE_ALREADY_BOOKED,
                    "Vehicle already has an overlapping confirmed appointment");
        }

        AvailableBay bay = repository.findFreeBay(dealership.id(), slot)
                .orElseThrow(() -> rejected(RejectionReason.NO_SERVICE_BAY_AVAILABLE,
                        "No active service bay is available for the full appointment"));
        AvailableTechnician technician = repository.findFreeTechnician(
                        dealership.id(), serviceType.requiredSkill(), slot)
                .orElseThrow(() -> rejected(RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE,
                        "No active qualified technician is available for the full appointment"));

        Instant createdAt = clock.instant().truncatedTo(ChronoUnit.MICROS);
        Appointment appointment = new Appointment(
                UUID.randomUUID(), dealership.id(), dealership.name(),
                serviceType.id(), serviceType.code(), serviceType.name(),
                bay.id(), bay.bayCode(), technician.id(), technician.fullName(),
                command.customerId(), command.vehicleVin(), start, end, "CONFIRMED", createdAt);

        try {
            repository.insert(appointment);
        } catch (DataIntegrityViolationException error) {
            throw translateExclusionConstraint(error);
        }

        return appointment;
    }

    @Transactional(readOnly = true)
    public Appointment get(UUID appointmentId) {
        return repository.findAppointment(appointmentId)
                .orElseThrow(() -> rejected(RejectionReason.APPOINTMENT_NOT_FOUND, "Appointment was not found"));
    }

    private BookingRejectedException translateExclusionConstraint(DataIntegrityViolationException error) {
        String databaseMessage = error.getMostSpecificCause().getMessage();
        if (databaseMessage != null && databaseMessage.contains("appointments_vin_no_overlap")) {
            return rejected(RejectionReason.VEHICLE_ALREADY_BOOKED,
                    "Vehicle already has an overlapping confirmed appointment");
        }
        if (databaseMessage != null && databaseMessage.contains("appointments_bay_no_overlap")) {
            return rejected(RejectionReason.NO_SERVICE_BAY_AVAILABLE,
                    "No active service bay is available for the full appointment");
        }
        if (databaseMessage != null && databaseMessage.contains("appointments_technician_no_overlap")) {
            return rejected(RejectionReason.NO_QUALIFIED_TECHNICIAN_AVAILABLE,
                    "No active qualified technician is available for the full appointment");
        }
        throw error;
    }

    private BookingRejectedException rejected(RejectionReason reason, String message) {
        return new BookingRejectedException(reason, message);
    }
}
