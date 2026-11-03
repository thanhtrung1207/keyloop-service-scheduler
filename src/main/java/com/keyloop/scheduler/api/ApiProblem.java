package com.keyloop.scheduler.api;

import io.swagger.v3.oas.annotations.media.Schema;

/** OpenAPI shape of the actual RFC 9457 response with its flattened errorCode extension. */
@Schema(description = "RFC 9457 Problem Details returned by the API")
public record ApiProblem(
        @Schema(example = "about:blank") String type,
        @Schema(example = "OUTSIDE_OPERATING_HOURS") String title,
        @Schema(example = "422") int status,
        @Schema(example = "The full appointment must fit within dealership opening hours") String detail,
        @Schema(example = "/api/v1/appointments") String instance,
        @Schema(requiredMode = Schema.RequiredMode.REQUIRED, example = "OUTSIDE_OPERATING_HOURS") String errorCode) { }
