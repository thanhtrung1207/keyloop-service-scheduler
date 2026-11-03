package com.keyloop.scheduler.api;

import com.keyloop.scheduler.booking.Appointment;
import com.keyloop.scheduler.booking.BookingCommand;
import com.keyloop.scheduler.booking.BookingService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
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
    @Operation(summary = "Book a vehicle service appointment")
    @ApiResponses({
            @ApiResponse(responseCode = "201", description = "Confirmed with an allocated bay and technician"),
            @ApiResponse(responseCode = "400", description = "Invalid request field or unsupported timestamp range"),
            @ApiResponse(responseCode = "404", description = "Unknown dealership or service type"),
            @ApiResponse(responseCode = "409", description = "Vehicle overlap or no eligible resource available"),
            @ApiResponse(responseCode = "422", description = "Start in the past or interval outside local opening hours")
    })
    public ResponseEntity<Appointment> create(@Valid @RequestBody CreateAppointmentRequest request) {
        Appointment appointment = bookingService.book(new BookingCommand(
                request.dealershipId(), request.customerId(), request.vehicleVin(),
                request.serviceTypeId(), request.desiredStartTime()));
        URI location = URI.create("/api/v1/appointments/" + appointment.id());
        return ResponseEntity.created(location).body(appointment);
    }

    @GetMapping("/{id}")
    @Operation(summary = "Read a confirmed appointment")
    @ApiResponses({@ApiResponse(responseCode = "200", description = "Appointment found"),
            @ApiResponse(responseCode = "404", description = "Unknown appointment")})
    public Appointment get(@PathVariable UUID id) {
        return bookingService.get(id);
    }

    public record CreateAppointmentRequest(
            @NotNull @Schema(example = "11111111-1111-1111-1111-111111111111") UUID dealershipId,
            @NotBlank @Size(max = 80) @Pattern(regexp = "^[^\\x00]+$", message = "must not contain NUL")
            @Schema(example = "CUST-DEMO-001") String customerId,
            @NotBlank @Pattern(regexp = "^[A-HJ-NPR-Z0-9]{17}$")
            @Schema(example = "WBA3A5C51CF256789") String vehicleVin,
            @NotNull @Schema(example = "44444444-4444-4444-4444-444444444401") UUID serviceTypeId,
            @NotNull @Schema(example = "2030-01-07T09:00:00Z") Instant desiredStartTime) { }
}
