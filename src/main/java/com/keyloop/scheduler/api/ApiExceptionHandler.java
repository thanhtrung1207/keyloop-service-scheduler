package com.keyloop.scheduler.api;

import com.keyloop.scheduler.booking.BookingRejectedException;
import com.keyloop.scheduler.booking.RejectionReason;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.stream.Collectors;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(BookingRejectedException.class)
    ProblemDetail bookingRejected(BookingRejectedException error) {
        return problem(statusFor(error.reason()), error.reason().name(), error.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail invalidRequest(MethodArgumentNotValidException error) {
        String detail = error.getBindingResult().getFieldErrors().stream()
                .map(fieldError -> fieldError.getField() + ": " + fieldError.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return problem(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", detail);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    ProblemDetail unreadableRequest(Exception error) {
        return problem(HttpStatus.BAD_REQUEST, "VALIDATION_FAILED", "Malformed JSON or invalid field value");
    }

    private ProblemDetail problem(HttpStatus status, String code, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(code);
        problem.setProperty("errorCode", code);
        return problem;
    }

    private HttpStatus statusFor(RejectionReason reason) {
        return switch (reason) {
            case DEALERSHIP_NOT_FOUND, SERVICE_TYPE_NOT_FOUND, APPOINTMENT_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case INVALID_VIN, INVALID_START_TIME -> HttpStatus.BAD_REQUEST;
            case START_TIME_IN_PAST -> HttpStatus.UNPROCESSABLE_ENTITY;
            case VEHICLE_ALREADY_BOOKED, NO_SERVICE_BAY_AVAILABLE,
                 NO_QUALIFIED_TECHNICIAN_AVAILABLE -> HttpStatus.CONFLICT;
        };
    }
}
