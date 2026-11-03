package com.keyloop.scheduler.api;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/appointments")
public class AppointmentController {
    private final BookingService bookingService;

    public AppointmentController(BookingService bookingService) {
        this.bookingService = bookingService;
    }

    @PostMapping
    public ResponseEntity<Appointment> create(@Valid @RequestBody CreateAppointmentRequest request) {
        Appointment appointment = bookingService.book(new BookingCommand(
                request.dealershipId(), request.customerId(), request.vehicleVin(),
                request.serviceTypeId(), request.desiredStartTime()));
        URI location = URI.create("/api/v1/appointments/" + appointment.id());
        return ResponseEntity.created(location).body(appointment);
    }

    @GetMapping("/{id}")
    public Appointment get(@PathVariable UUID id) {
        return bookingService.get(id);
    }

    public record CreateAppointmentRequest(
            @NotNull UUID dealershipId,
            @NotBlank @Size(max = 80) @Pattern(regexp = "^[^\\x00]+$", message = "must not contain NUL") String customerId,
            @NotBlank @Pattern(regexp = "^[A-HJ-NPR-Z0-9]{17}$") String vehicleVin,
            @NotNull UUID serviceTypeId,
            @NotNull Instant desiredStartTime) { }
}
